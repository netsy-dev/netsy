// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/netsy-dev/netsy/internal/heartbeat"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/metrics"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/peerclient"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/watch"
)

// reconnectDelay is the wait time before retrying a Follow stream
// connection after a failure.
const reconnectDelay = time.Second

// Follower connects to the Primary's Follow RPC as a Replica, receives
// PrimaryMessages, persists records locally, and sends Receipts.
type Follower struct {
	logger          *slog.Logger
	nodeID          string
	state           *nodestate.State
	peers           *peerclient.Manager
	db              localdb.Database
	heartbeatSender *heartbeat.Sender
	watchManager    *watch.Manager

	mu                  sync.Mutex
	cancel              context.CancelFunc
	requireInitialSync  bool
	initialSyncWaiterCh chan error

	lagCheckMu     sync.Mutex
	lagCheckCancel context.CancelFunc
	lagCheckSeq    uint64

	metrics           *Metrics
	compactionMetrics *metrics.CompactionMetrics
	retryMetrics      *metrics.RetryMetrics
}

// SetMetrics sets the Replica-scoped metrics reference.
func (f *Follower) SetMetrics(m *Metrics) {
	f.metrics = m
}

// NewFollower creates a new replication Follower.
func NewFollower(
	logger *slog.Logger,
	nodeID string,
	state *nodestate.State,
	peers *peerclient.Manager,
	db localdb.Database,
	heartbeatSender *heartbeat.Sender,
	watchManager *watch.Manager,
	replicaMetrics *Metrics,
	compactionMetrics *metrics.CompactionMetrics,
	retryMetrics *metrics.RetryMetrics,
) *Follower {
	return &Follower{
		logger:            logger,
		nodeID:            nodeID,
		state:             state,
		peers:             peers,
		db:                db,
		heartbeatSender:   heartbeatSender,
		watchManager:      watchManager,
		metrics:           replicaMetrics,
		compactionMetrics: compactionMetrics,
		retryMetrics:      retryMetrics,
	}
}

// RequireInitialSync marks the next Start call as a bootstrap start that must
// not return until the follower has either received the Primary's Initial
// message or determined that no remote Primary follow stream is needed.
func (f *Follower) RequireInitialSync() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.requireInitialSync = true
}

// Start begins the Follow stream loop in a background goroutine. It is a
// no-op if the follower is already running. When RequireInitialSync was called
// beforehand, Start blocks until that initial synchronization requirement has
// been satisfied.
func (f *Follower) Start(parent context.Context) error {
	f.mu.Lock()

	waitCh := f.initialSyncWaiterCh
	if f.cancel != nil {
		f.mu.Unlock()
		if waitCh != nil {
			return <-waitCh
		}
		return nil
	}

	if f.requireInitialSync {
		waitCh = make(chan error, 1)
		f.initialSyncWaiterCh = waitCh
		f.requireInitialSync = false
	}

	ctx, cancel := context.WithCancel(parent)
	f.cancel = cancel
	f.mu.Unlock()

	go f.run(ctx)
	f.logger.Info("follower started")

	if waitCh != nil {
		return <-waitCh
	}
	return nil
}

// Stop cancels the Follow stream loop. It is a no-op if the follower
// is not running.
func (f *Follower) Stop() {
	f.mu.Lock()
	if f.cancel == nil {
		f.mu.Unlock()
		return
	}

	waitCh := f.initialSyncWaiterCh
	f.cancel()
	f.cancel = nil
	f.initialSyncWaiterCh = nil
	f.mu.Unlock()

	f.cancelCommittedRevisionLagCheck()
	f.logger.Info("follower stopped")

	if waitCh != nil {
		f.finishInitialSyncWait(waitCh, context.Canceled)
	}
}

// run is the internal reconnect loop.
func (f *Follower) run(ctx context.Context) {
	initialPending := true
	initialResult := f.initialWaiter()
	if initialResult == nil {
		initialPending = false
	}

	for {
		select {
		case <-ctx.Done():
			if initialPending {
				f.finishInitialSyncWait(initialResult, ctx.Err())
			}
			return
		default:
		}

		initialDelivered, err := f.connectAndFollow(ctx, initialResult, initialPending)
		if initialDelivered {
			initialPending = false
		}
		if err != nil {
			f.logger.Warn("follow stream ended", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectDelay):
		}
	}
}

func (f *Follower) initialWaiter() chan error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.initialSyncWaiterCh
}

// connectAndFollow attempts one Follow stream session. The returned boolean
// reports whether the bootstrap caller's initialResult has been satisfied.
func (f *Follower) connectAndFollow(ctx context.Context, initialResult chan error, initialPending bool) (bool, error) {
	cs := f.state.ClusterState()

	// Skip if no Primary, or if this node is the Primary.
	if cs.Primary.NodeID == "" || cs.Primary.NodeID == f.nodeID {
		if initialPending {
			f.finishInitialSyncWait(initialResult, nil)
		}
		return initialPending, nil
	}

	// Validate the Primary's state before streaming. Replicas must
	// reject data from a node whose Primary State is Replica.
	remoteState, err := f.peers.GetNodeState(ctx, cs.Primary.PeerAdvertiseAddr)
	if err != nil {
		return false, err
	}
	remotePrimary := nodestate.PrimaryFromProto(remoteState.GetPrimaryState())
	if remotePrimary == nodestate.PrimaryReplica {
		f.logger.Warn("remote node is not primary, skipping follow",
			"remote_node_id", cs.Primary.NodeID,
			"remote_primary_state", remotePrimary,
		)
		return false, fmt.Errorf("remote node %s is not acting as primary", cs.Primary.NodeID)
	}

	client := f.peers.PrimaryClient()
	if client == nil {
		return false, fmt.Errorf("primary client is not connected")
	}

	stream, err := client.Follow(ctx)
	if err != nil {
		return false, err
	}

	f.logger.Info("follow stream established", "primary", cs.Primary.NodeID)

	// Discard any pending watch events from a previous stream since
	// they are stale — the new Initial message will set the
	// authoritative committed revision.
	if f.watchManager != nil {
		f.watchManager.ResetPending()
	}

	return f.processStream(stream, initialResult, initialPending)
}

// processStream handles all messages on an active Follow stream. The returned
// boolean reports whether the bootstrap caller's initialResult has been
// satisfied.
func (f *Follower) processStream(stream proto.Primary_FollowClient, initialResult chan error, initialPending bool) (bool, error) {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			if initialPending {
				return false, fmt.Errorf("follow stream ended before initial message")
			}
			return true, nil
		}
		if err != nil {
			return !initialPending, err
		}

		switch payload := msg.Payload.(type) {
		case *proto.PrimaryMessage_Initial:
			if err := f.handleInitial(payload.Initial); err != nil {
				if initialPending {
					f.finishInitialSyncWait(initialResult, err)
					initialPending = false
				}
				return !initialPending, err
			}
			if initialPending {
				f.finishInitialSyncWait(initialResult, nil)
				initialPending = false
			}

		case *proto.PrimaryMessage_Record:
			if err := f.handleRecord(stream, payload.Record); err != nil {
				return !initialPending, err
			}

		case *proto.PrimaryMessage_Commit:
			f.handleCommit(payload.Commit)

		case *proto.PrimaryMessage_Compact:
			if err := f.handleCompact(payload.Compact); err != nil {
				return !initialPending, err
			}
		}
	}
}

// finishInitialSyncWait unblocks the bootstrap caller waiting for the first
// stream outcome and clears the waiter when it still matches the active one.
func (f *Follower) finishInitialSyncWait(waitCh chan error, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.initialSyncWaiterCh == waitCh {
		f.initialSyncWaiterCh = nil
	}
	waitCh <- err
}

// handleInitial processes the Initial message sent by the Primary at the
// start of each Follow stream, restoring committed state immediately and
// durably seeding compaction state if the local node had not persisted one yet.
func (f *Follower) handleInitial(initial *proto.Initial) error {
	if initial.GetCompactionRevision() > 0 {
		if current, err := f.db.LatestCompactionRevision(); err != nil {
			return err
		} else if current == 0 {
			if err := f.db.PersistCompactionRevision(initial.GetCompactionRevision()); err != nil {
				return err
			}
		}
	}

	f.state.SetCommitted(initial.GetCommittedRevision())
	f.state.SetCompaction(initial.GetCompactionRevision())

	f.logger.Info("received initial",
		"committed_revision", initial.GetCommittedRevision(),
		"compaction_revision", initial.GetCompactionRevision(),
	)

	if f.watchManager != nil {
		f.watchManager.AdvanceCommittedRevision(initial.GetCommittedRevision())
	}

	return nil
}

// handleRecord persists a replicated record to the local database,
// enqueues the revision for watch delivery, and sends a receipt with
// an embedded heartbeat back to the Primary. Records above the current
// committed revision are treated as tentative and can be overwritten if
// the Primary retries with a different record at the same revision
// (same-revision retry after quorum rollback).
func (f *Follower) handleRecord(stream proto.Primary_FollowClient, record *proto.Record) error {
	// ReplicateTentativeRecord allows overwriting tentative records above
	// committed_revision while preserving strict idempotency for committed ones.
	if _, err := f.db.ReplicateTentativeRecord(record, f.state.Committed()); err != nil {
		f.logger.Error("failed to replicate record", "revision", record.GetRevision(), "error", err)
		return err
	}

	// Buffer the revision for watch delivery — it will be read from
	// the database and delivered when the commit message arrives.
	if f.watchManager != nil {
		f.watchManager.EnqueueWatchRevision(record.GetRevision())
	}

	// Send receipt with embedded heartbeat back to the Primary.
	receipt := &proto.ReplicaMessage{
		Revision:  record.GetRevision(),
		Heartbeat: f.heartbeatSender.BuildNodeState(),
	}

	if err := stream.Send(receipt); err != nil {
		f.logger.Warn("failed to send receipt",
			"attempt", 1,
			"revision", receipt.GetRevision(),
			"error", err,
		)

		if f.retryMetrics != nil {
			f.retryMetrics.Inc("receipt_send")
		}
		if err := stream.Send(receipt); err != nil {
			f.logger.Warn("failed to send receipt",
				"attempt", 2,
				"revision", receipt.GetRevision(),
				"error", err,
			)
			f.degradeSelf("receipt send failed after retry", err)
			return err
		}
	}

	f.heartbeatSender.MarkReceiptSent()
	if f.metrics != nil {
		f.metrics.ReceiptAge.MarkSent()
	}
	return nil
}

// handleCommit advances the local committed revision, notifies the watch
// subsystem so buffered records up to this revision can be delivered, and
// schedules replica lag checking once the node is Healthy.
func (f *Follower) handleCommit(committedRevision int64) {
	f.state.SetCommitted(committedRevision)

	if f.watchManager != nil {
		f.watchManager.AdvanceCommittedRevision(committedRevision)
	}

	if f.state.Health() == nodestate.HealthHealthy {
		f.scheduleCommittedRevisionLagCheck(committedRevision)
	}
}

// handleCompact persists the Primary's compaction revision update,
// updates in-memory state, and enqueues async compaction execution.
func (f *Follower) handleCompact(compactionRevision int64) error {
	if err := f.db.PersistCompactionRevision(compactionRevision); err != nil {
		return err
	}

	f.state.SetCompaction(compactionRevision)

	f.logger.Info("received compaction revision update",
		"compaction_revision", compactionRevision,
	)

	go func() {
		start := time.Now()
		affected, err := f.db.ExecuteCompaction(compactionRevision)
		if err != nil {
			if f.compactionMetrics != nil {
				f.compactionMetrics.Duration.WithLabelValues("error").Observe(time.Since(start).Seconds())
			}
			f.logger.Error("compaction execution failed",
				"compaction_revision", compactionRevision, "error", err,
			)
			return
		}
		if f.compactionMetrics != nil {
			f.compactionMetrics.Duration.WithLabelValues("success").Observe(time.Since(start).Seconds())
		}
		f.logger.Info("compaction execution completed",
			"compaction_revision", compactionRevision, "records_compacted", affected,
		)
	}()

	return nil
}
