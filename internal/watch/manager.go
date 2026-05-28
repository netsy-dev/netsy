// Netsy <https://netsy.dev>
// Copyright 2026 Nadrama Pty Ltd
// SPDX-License-Identifier: Apache-2.0

package watch

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/netsy-dev/netsy/internal/proto"
	"go.etcd.io/etcd/api/v3/mvccpb"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// RecordReader provides read access to records by revision for watch
// event delivery.
type RecordReader interface {
	FindRecordByRev(revision int64) (*proto.Record, error)
}

// Manager owns all active watchers and handles event distribution.
// It is shared by the "clientapi" (which creates watchers), the "primary"
// write path (which distributes events after commits), and the
// "replication" follower (which buffers and delivers replicated events).
type Manager struct {
	logger *slog.Logger
	db     RecordReader

	// mu guards the watchers map. Distribute holds a read lock;
	// Register and Unregister hold a write lock.
	mu       sync.RWMutex
	watchers map[int64]*Watcher

	// watchAdmissionFloor gates new Watch requests; revisions below
	// this are rejected. Raised atomically during compaction notice
	// acceptance and persisted durably after cluster-wide confirmation.
	watchAdmissionFloor atomic.Int64

	pendingMu sync.Mutex
	pending   map[int64]struct{}
}

// NewManager creates a new watch Manager.
func NewManager(logger *slog.Logger, db RecordReader) *Manager {
	return &Manager{
		logger:   logger,
		db:       db,
		watchers: make(map[int64]*Watcher),
		pending:  make(map[int64]struct{}),
	}
}

// Register adds a watcher to the manager and returns its assigned ID.
func (m *Manager) Register(w *Watcher) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.watchers[w.id] = w
	return w.id
}

// Unregister removes a watcher from the manager.
func (m *Manager) Unregister(id int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.watchers, id)
}

// Distribute delivers a Record event to all watchers with matching watches.
func (m *Manager) Distribute(record *proto.Record, prevRecord *proto.Record) {
	if record == nil {
		return
	}

	eventType := mvccpb.PUT
	if record.Deleted {
		eventType = mvccpb.DELETE
	}

	// note: WatchId is set in the watches loop (below), this is a msg template
	msg := pb.WatchResponse{
		Header: &pb.ResponseHeader{
			Revision: record.Revision,
		},
		Events: []*mvccpb.Event{
			{
				Type: eventType,
				Kv: &mvccpb.KeyValue{
					Key:            record.Key,
					CreateRevision: record.CreateRevision,
					ModRevision:    record.Revision,
					Version:        record.Version,
					Value:          record.Value,
					Lease:          record.Lease,
				},
			},
		},
	}

	// note: this value will not be set if prevRecord has already
	// been compacted. we also do not set on the msg struct
	// directly as not all watches will request prev_kv=true.
	var msgPrevKv *mvccpb.KeyValue
	if prevRecord != nil {
		msgPrevKv = &mvccpb.KeyValue{
			Key:            prevRecord.Key,
			CreateRevision: prevRecord.CreateRevision,
			ModRevision:    prevRecord.Revision,
			Version:        prevRecord.Version,
			Value:          prevRecord.Value,
			Lease:          prevRecord.Lease,
		}
	}

	m.mu.RLock()

	var slowWatchers []*Watcher
	for _, w := range m.watchers {
		slow := false
		w.RLock()
		for watchID, watch := range w.watches {
			if isWatchMatch(watch, record) {
				ev := *msg.Events[0]
				if watch.prevKv {
					ev.PrevKv = msgPrevKv
				} else {
					ev.PrevKv = nil
				}
				outMsg := msg
				outMsg.Events = []*mvccpb.Event{&ev}
				outMsg.WatchId = watchID
				select {
				case w.inboxCh <- outMsg:
				default:
					// Preserve the watch stream's no-gap contract by failing the
					// slow stream instead of dropping one committed event.
					m.logger.Warn("closing slow watch stream: watcher inbox full",
						"watcher_id", w.id,
						"watch_id", watchID,
						"revision", record.Revision,
					)
					slow = true
				}
				if slow {
					break
				}
			}
		}
		w.RUnlock()
		if slow {
			slowWatchers = append(slowWatchers, w)
		}
	}
	m.mu.RUnlock()

	for _, w := range slowWatchers {
		w.Cleanup(m, m.logger)
	}
}

// EnqueueWatchRevision buffers a revision for watch delivery once
// committed_revision advances past it. Used by Replicas that receive
// records before the corresponding commit message.
func (m *Manager) EnqueueWatchRevision(revision int64) {
	if revision <= 0 {
		return
	}

	m.pendingMu.Lock()
	m.pending[revision] = struct{}{}
	m.pendingMu.Unlock()
}

// ResetPending discards all buffered revisions. Called when the
// replication stream reconnects, since any pending entries from the
// previous stream are stale.
func (m *Manager) ResetPending() {
	m.pendingMu.Lock()
	m.pending = make(map[int64]struct{})
	m.pendingMu.Unlock()
}

// AdvanceCommittedRevision delivers all pending watch events with
// revisions up to and including rev, in ascending revision order.
func (m *Manager) AdvanceCommittedRevision(rev int64) {
	m.pendingMu.Lock()

	var ready []int64
	for r := range m.pending {
		if r <= rev {
			ready = append(ready, r)
		}
	}

	if len(ready) == 0 {
		m.pendingMu.Unlock()
		return
	}

	sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })

	for _, r := range ready {
		delete(m.pending, r)
	}
	m.pendingMu.Unlock()

	for _, r := range ready {
		m.distributeFromDB(r)
	}
}

// WatchCount returns the total number of active watches across all
// watchers on this node.
func (m *Manager) WatchCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count := 0
	for _, w := range m.watchers {
		w.RLock()
		count += len(w.watches)
		w.RUnlock()
	}
	return count
}

// MinWatchRevision returns the lowest startRevision across all active
// watches on this node. If no watches are active, it returns -1 to
// signal the caller should fall back to committed revision.
func (m *Manager) MinWatchRevision() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var minRev int64 = -1
	for _, w := range m.watchers {
		w.RLock()
		for _, entry := range w.watches {
			if minRev < 0 || entry.startRevision < minRev {
				minRev = entry.startRevision
			}
		}
		w.RUnlock()
	}
	return minRev
}

// WatchAdmissionFloor returns the current watch-admission floor revision.
func (m *Manager) WatchAdmissionFloor() int64 {
	return m.watchAdmissionFloor.Load()
}

// SetWatchAdmissionFloor atomically sets the watch-admission floor to
// the given revision, then validates that no existing active watch has
// a startRevision below it. If validation fails, the floor is rolled
// back to its previous value and an error is returned.
func (m *Manager) SetWatchAdmissionFloor(revision int64) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	previous := m.watchAdmissionFloor.Load()
	m.watchAdmissionFloor.Store(revision)

	for _, w := range m.watchers {
		w.RLock()
		for _, entry := range w.watches {
			if entry.startRevision > 0 && entry.startRevision < revision {
				w.RUnlock()
				m.watchAdmissionFloor.Store(previous)
				return fmt.Errorf("active watch exists below proposed compaction revision %d", revision)
			}
		}
		w.RUnlock()
	}

	return nil
}

// distributeFromDB reads a record from SQLite by revision and delivers
// it to matching watchers, including the previous record for watches
// that request prev_kv.
func (m *Manager) distributeFromDB(revision int64) {
	record, err := m.db.FindRecordByRev(revision)
	if err != nil {
		m.logger.Warn("failed to read record for watch delivery", "revision", revision, "error", err)
		return
	}

	var prevRecord *proto.Record
	if !record.Created && record.PrevRevision > 0 {
		prevRecord, err = m.db.FindRecordByRev(record.PrevRevision)
		if err != nil {
			m.logger.Debug("failed to read prev record for watch delivery", "revision", revision, "prev_revision", record.PrevRevision, "error", err)
		}
	}

	m.Distribute(record, prevRecord)
}
