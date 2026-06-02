// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package watch

// glossary
// watcher - watchers represents a single gRPC bidirectional stream client
//           e.g. kube-apiserver
// watch   - watches range on/track specific events. multiple per watcher.
//           e.g. multiple `kubectl watch` commands connected to a
//                single kube-apiserver watcher.

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/netsy-dev/netsy/internal/proto"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// watcherIDCounter is a global counter for watcher IDs.
var watcherIDCounter int64

// watchIDCounter is a global counter for watch IDs.
// We do not support client-supplied watch IDs.
var watchIDCounter int64

// Watcher is a watch server that handles requests from a single client,
// where each client may have one or more 'watch(es)' and each 'watch' may have
// progress notifications enabled.
// client is a gRPC bidirectional stream
// inboxCh is used to send WatchResponse messages to the watcher
// data flow (where brackets represent other components):
// (kubeapi-server) > client.Recv > Get[Create|Cancel|Progress]Request > (api)
// (netsy Leader) > inboxCh > client.Send > (kube-apiserver) [> watcher client]
type Watcher struct {
	id int64
	sync.RWMutex
	client  pb.Watch_WatchServer // the gRPC stream
	sendMu  sync.Mutex           // serializes all Send calls on the gRPC stream
	inboxOk bool
	// inboxCh buffers responses for the stream sender. Committed watch events
	// are no-gap: a full inbox closes the watcher instead of dropping an event.
	inboxCh  chan pb.WatchResponse
	doneCh   chan struct{}
	watches  map[int64]watchEntry
	progress map[int64]bool
}

// watchEntry holds information from a CreateWatchRequest, plus a
// context cancellation function.
type watchEntry struct {
	key             []byte
	rangeEnd        []byte
	startRevision   int64
	prevKv          bool
	progressNotify  bool
	filtersNoPut    bool
	filtersNoDelete bool
	cancel          func()
}

// NewWatcher creates a new Watcher with a globally-unique ID for the
// given gRPC watch stream.
func NewWatcher(ws pb.Watch_WatchServer) *Watcher {
	// Buffer short bursts from the write path while the gRPC sender drains.
	// Manager.Distribute treats a full buffer as a slow watcher and evicts it;
	// progress notifications are advisory and may be skipped on overflow.
	const inboxCapacity = 64

	return &Watcher{
		id:       atomic.AddInt64(&watcherIDCounter, 1),
		client:   ws,
		inboxOk:  true,
		inboxCh:  make(chan pb.WatchResponse, inboxCapacity),
		doneCh:   make(chan struct{}),
		watches:  map[int64]watchEntry{},
		progress: map[int64]bool{},
	}
}

// ID returns the watcher's unique identifier.
func (w *Watcher) ID() int64 {
	return w.id
}

// Client returns the watcher's gRPC stream.
func (w *Watcher) Client() pb.Watch_WatchServer {
	return w.client
}

// InboxCh returns the channel used to send WatchResponse messages to
// the watcher's dispatch goroutine.
func (w *Watcher) InboxCh() <-chan pb.WatchResponse {
	return w.inboxCh
}

// Send serializes writes to the gRPC stream. gRPC ServerStream.Send
// is not safe for concurrent use; all callers must use this method
// instead of calling w.client.Send directly.
func (w *Watcher) Send(resp *pb.WatchResponse) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	return w.client.Send(resp)
}

// Done is closed when the watcher shuts down, including server-side closure
// of a slow watcher whose inbox stopped draining.
func (w *Watcher) Done() <-chan struct{} {
	return w.doneCh
}

// Cleanup removes the watcher from the manager, closes its inbox, and cancels
// all watches. It is idempotent because both the client handler and the
// server-side slow-watcher path can race to close the same watcher.
func (w *Watcher) Cleanup(m *Manager, logger *slog.Logger) {
	logger.Debug("watcher cleanup", "watcher_id", w.id)

	m.Unregister(w.id)

	w.Lock()
	if !w.inboxOk {
		w.Unlock()
		return
	}

	w.inboxOk = false
	close(w.inboxCh)
	close(w.doneCh)

	// remove all watchIDs from watcher (in case Cancel was not processed)
	for watchID, watch := range w.watches {
		watch.cancel()
		delete(w.watches, watchID)
	}
	for watchID := range w.progress {
		delete(w.progress, watchID)
	}
	w.Unlock()
}

// CreateWatch handles watch create requests. Admission is gated by the
// higher of the persisted compaction revision and the watch-admission
// floor (which may be provisionally raised during a compaction notice).
// Any request for a startRevision at or below that floor is rejected.
// The getAdmissionFloor function is re-evaluated under the watcher's
// write lock to prevent TOCTOU races with SetWatchAdmissionFloor.
func (w *Watcher) CreateWatch(r *pb.WatchCreateRequest, latestRevision int64, compactionRevision int64, getAdmissionFloor func() int64, getRevision func(findRevision int64) (revision int64, compacted bool, compactedAt sql.NullString, err error), logger *slog.Logger) {
	logger.Debug("create watch", "watcher_id", w.id)

	respHeader := &pb.ResponseHeader{
		Revision: latestRevision,
	}

	// Use the higher of the persisted compaction revision and the
	// watch-admission floor as the effective compaction floor.
	if floor := getAdmissionFloor(); floor > compactionRevision {
		compactionRevision = floor
	}

	// Reject watches requesting a revision below the compaction floor.
	if r.StartRevision > 0 && compactionRevision > 0 && r.StartRevision <= compactionRevision {
		watchID := atomic.AddInt64(&watchIDCounter, 1)
		cancelReason := fmt.Sprintf("required revision %d has been compacted; compaction revision is %d", r.StartRevision, compactionRevision)
		logger.Debug("create watch rejected by compaction floor",
			"start_revision", r.StartRevision,
			"compaction_revision", compactionRevision,
		)
		_ = w.Send(&pb.WatchResponse{
			Header:  respHeader,
			Created: true,
			WatchId: watchID,
		})
		_ = w.Send(&pb.WatchResponse{
			Header:          respHeader,
			Canceled:        true,
			CancelReason:    cancelReason,
			CompactRevision: compactionRevision,
			WatchId:         watchID,
		})
		return
	}

	// do not support user-provided watch IDs
	if r.WatchId != clientv3.AutoWatchID {
		logger.Warn("user-provided watch IDs are unsupported", "watch_id", r.WatchId)
		_ = w.Send(&pb.WatchResponse{
			Header:  respHeader,
			Created: true,
			WatchId: r.WatchId,
		})
		_ = w.Send(&pb.WatchResponse{
			Header:       respHeader,
			Canceled:     true,
			CancelReason: "user-provided watch IDs are unsupported",
			WatchId:      r.WatchId,
		})
		return
	}

	// create a globally-unique watch ID
	watchID := atomic.AddInt64(&watchIDCounter, 1)

	// get cancel function associated with watch server
	_, cancelFunc := context.WithCancel(w.client.Context())

	// Check if start revision exists or has been compacted.
	// If set to zero, use latest (committed) revision.
	var revision int64
	var compacted bool
	var err error
	if r.StartRevision == 0 {
		revision = latestRevision
	} else {
		revision, compacted, _, err = getRevision(r.StartRevision)
	}
	respHeader.Revision = revision
	if err != nil || compacted {
		var cancelReason string
		var compactRevision int64
		if compacted {
			compactRevision = r.StartRevision
			cancelReason = fmt.Sprintf("revision '%d' has been compacted", r.StartRevision)
		} else if r.StartRevision <= latestRevision {
			respHeader.Revision = r.StartRevision
			cancelReason = fmt.Sprintf("failed to get revision '%d' for CreateWatch: %v", r.StartRevision, err)
		} else {
			// if asking for future revision, use latest
			revision = latestRevision
		}
		if cancelReason != "" {
			logger.Debug("create watch failed", "reason", cancelReason, "start_revision", r.StartRevision)
			_ = w.Send(&pb.WatchResponse{
				Header:  respHeader,
				Created: true,
				WatchId: watchID,
			})
			_ = w.Send(&pb.WatchResponse{
				Header:          respHeader,
				Canceled:        true,
				CancelReason:    cancelReason,
				CompactRevision: compactRevision,
				WatchId:         watchID,
			})
			cancelFunc()
			return
		}
	}

	// prep watch
	watchData := watchEntry{
		key:            r.Key,
		rangeEnd:       r.RangeEnd,
		startRevision:  r.StartRevision,
		prevKv:         r.PrevKv,
		progressNotify: r.ProgressNotify,
		cancel:         cancelFunc,
	}
	for _, filterType := range r.Filters {
		switch filterType {
		case pb.WatchCreateRequest_NOPUT:
			watchData.filtersNoPut = true
		case pb.WatchCreateRequest_NODELETE:
			watchData.filtersNoDelete = true
		}
	}

	// add watchID to the watcher
	// obtain write lock, re-check admission floor, add, then release lock
	w.Lock()
	if !w.inboxOk {
		w.Unlock()
		cancelFunc()
		return
	}
	if r.StartRevision > 0 {
		if floor := getAdmissionFloor(); floor > 0 && r.StartRevision <= floor {
			w.Unlock()
			cancelReason := fmt.Sprintf("required revision %d has been compacted; compaction revision is %d", r.StartRevision, floor)
			logger.Debug("create watch rejected by compaction floor (re-check)",
				"start_revision", r.StartRevision,
				"compaction_revision", floor,
			)
			_ = w.Send(&pb.WatchResponse{
				Header:  respHeader,
				Created: true,
				WatchId: watchID,
			})
			_ = w.Send(&pb.WatchResponse{
				Header:          respHeader,
				Canceled:        true,
				CancelReason:    cancelReason,
				CompactRevision: floor,
				WatchId:         watchID,
			})
			cancelFunc()
			return
		}
	}
	w.watches[watchID] = watchData
	w.progress[watchID] = r.ProgressNotify
	w.Unlock()

	// acknowledge the watch create request to the client
	if err := w.Send(&pb.WatchResponse{
		Header:  respHeader,
		Created: true,
		WatchId: watchID,
	}); err != nil {
		// cancel watch if unable to send ack
		w.CancelWatch(watchID, revision, err, logger)
		return
	}
}

// CancelWatch handles watch cancel requests for a watch server instance.
// It may be called from multiple different goroutines.
// Arguments:
//   - revision: latest known revision to place in response header.
//   - reason: if watch is being cancelled due to an unexpected error.
func (w *Watcher) CancelWatch(watchID int64, revision int64, reason error, logger *slog.Logger) {
	logger.Debug("cancel watch", "watcher_id", w.id, "watch_id", watchID)

	// remove watchID from watcher
	// obtain write lock, cancel, delete, then release lock immediately
	w.Lock()
	if watch, ok := w.watches[watchID]; ok {
		watch.cancel()
	}
	delete(w.watches, watchID)
	delete(w.progress, watchID)
	w.Unlock()

	// ack cancellation with client
	reasonMsg := ""
	if reason != nil {
		reasonMsg = reason.Error()
	}
	err := w.Send(&pb.WatchResponse{
		Header: &pb.ResponseHeader{
			Revision: revision,
		},
		Canceled:     reason != nil,
		CancelReason: reasonMsg,
		WatchId:      watchID,
	})
	if err != nil && reason != nil && !clientv3.IsConnCanceled(err) {
		logger.Warn("failed to send cancel to watch", "watch_id", watchID, "error", err)
	}
}

// ReportProgressOnInterval sends a progress report (aka the latest revision)
// on an interval to all watchers which have progress notifications enabled.
// This function is triggered by PollUntilContextCancel, hence we always return
// false for the condition and nil for the error, to permit it to continue polling.
// It obtains a read lock in order to check which watch IDs have progress
// notifications enabled. It then writes one message for each watch to the
// dispatch channel for the main watcher goroutine to handle sending back
// to the watcher client. If all watches have progress notifications enabled,
// instead of sending multiple messages, it sends a broadcast message.
// Note that this function is also used for on-demand progress requests.
func (w *Watcher) ReportProgressOnInterval(committedRevision func() int64, logger *slog.Logger) func(ctx context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		// get committed revision
		revision := committedRevision()

		// create array of watchIDs to send to
		progressWatchIDs := make([]int64, 0)
		broadcast := true

		// get a read lock on the watcher to ensure inbox channel is not closed
		// release at the end of the function
		w.RLock()
		defer w.RUnlock()

		// check that inbox channel is not closed
		if !w.inboxOk {
			return false, nil
		}

		// determine which watch IDs have progress notifications enabled.
		// set broadcast to false if any of the watches have progress notifications
		// disabled.
		// obtain read lock, iterate on progress map, then release lock immediately
		for watchID, progressNotify := range w.progress {
			if progressNotify {
				progressWatchIDs = append(progressWatchIDs, watchID)
			} else {
				broadcast = false
			}
		}

		// Progress notifications are advisory watermarks, not entries in the
		// event history; a later tick can supersede a skipped progress response.
		if broadcast {
			// send a single watch response to the dispatch channel
			select {
			case w.inboxCh <- pb.WatchResponse{
				Header: &pb.ResponseHeader{
					Revision: revision,
				},
				// using an invalid watch ID makes it a broadcast
				WatchId: clientv3.InvalidWatchID,
			}:
			default:
				logger.Warn("skipping watch progress notification: watcher inbox full",
					"watcher_id", w.id,
					"revision", revision,
				)
			}
		} else {
			// send a watch response for each watch ID to the dispatch channel
			for _, watchID := range progressWatchIDs {
				select {
				case w.inboxCh <- pb.WatchResponse{
					Header: &pb.ResponseHeader{
						Revision: revision,
					},
					WatchId: watchID,
				}:
				default:
					logger.Warn("skipping watch progress notification: watcher inbox full",
						"watcher_id", w.id,
						"watch_id", watchID,
						"revision", revision,
					)
				}
			}
		}

		// always return condition=false, err=nil
		return false, nil
	}
}

// isWatchMatch checks if a watch should be sent a record based on its filters properties.
func isWatchMatch(w watchEntry, record *proto.Record) bool {
	// ignore put actions if 'noPut' filter is set
	if w.filtersNoPut && !record.Deleted {
		return false
	}

	// ignore delete actions if 'noDelete' filter is set
	if w.filtersNoDelete && record.Deleted {
		return false
	}

	// ignore if revision is greater than watch startRevision
	if w.startRevision > record.Revision {
		return false
	}

	// match if key is 'in range'
	if isInRange(record.Key, w.key, w.rangeEnd) {
		return true
	}

	// default to false
	return false
}

// isInRange checks if a key is in the range e.g. of a watch.
func isInRange(key []byte, rangeKey []byte, rangeEnd []byte) bool {
	// determine case (similar to etcdapi_kv_range.go Range)
	zeroByte := []byte{0}
	rangeKeyAndZeroByte := append(rangeKey, byte(0))
	var rangeEndPrefixValue []byte
	if len(key) > 0 {
		rangeKeyCopy := make([]byte, len(rangeKey))
		copy(rangeKeyCopy, rangeKey)
		rangeEndPrefixValue = append(
			rangeKeyCopy[:len(rangeKeyCopy)-1],
			rangeKeyCopy[len(rangeKeyCopy)-1]+1,
		)
	}
	if len(rangeEnd) == 0 || bytes.Equal(rangeEnd, rangeKeyAndZeroByte) {
		// check for exact match
		if bytes.Equal(key, bytes.TrimRight(rangeKey, "\x00")) {
			return true
		}
	} else if bytes.Equal(rangeKey, zeroByte) && bytes.Equal(rangeEnd, zeroByte) {
		// both keys are zero bytes, true for all keys
		return true
	} else if bytes.Equal(rangeEnd, zeroByte) {
		// rangeEnd is zero bytes, true for all keys greater than or equal to
		// range key
		// key_blob >= r.Key
		if bytes.Compare(key, rangeKey) >= 0 {
			// key=abc, rangeKey=abd, compare=-1 ("key less than rangeKey")
			// key=abc, rangeKey=abc, compare=0 ("key equal to rangeKey")
			// key=abc, rangeKey=abb, compare=1 ("key greater than rangeKey")
			return true
		}
	} else if rangeEndPrefixValue != nil && bytes.Equal(rangeEnd, rangeEndPrefixValue) {
		// check if key matches prefix, where rangeKey is the prefix
		if bytes.HasPrefix(key, rangeKey) {
			return true
		}
	} else {
		// range; check if key is greater than or equal to rangeKey, and less than rangeEnd
		// key_blob >= r.Key
		// AND key_blob < r.RangeEnd
		if bytes.Compare(key, rangeKey) >= 0 && bytes.Compare(key, rangeEnd) < 0 {
			return true
		}
	}
	return false
}
