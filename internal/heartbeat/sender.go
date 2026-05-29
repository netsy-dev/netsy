// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package heartbeat

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/netsy-dev/netsy/internal/metrics"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/peerclient"
	"github.com/netsy-dev/netsy/internal/proto"
)

// RevisionSource provides the latest revision for heartbeat messages.
type RevisionSource interface {
	LatestRevision() (int64, error)
}

// PrimarySelfDegradeFunc is called when this node is the Primary and it
// self-degrades due to heartbeat failure. The callback should trigger the
// drain-flush-resign sequence asynchronously.
type PrimarySelfDegradeFunc func()

// Sender sends heartbeats to the Elector and Primary on a single
// cadence. It uses the following routing logic:
//
//   - To the Elector: always send on heartbeat_interval.
//   - To the Primary: send on heartbeat_interval only when
//     no Receipt has been sent within that window.
//   - When Elector == Primary: a single heartbeat satisfies both.
type Sender struct {
	logger    *slog.Logger
	nodeID    string
	state     *nodestate.State
	peers     *peerclient.Manager
	db        RevisionSource
	startTime int64
	interval  time.Duration

	onPrimarySelfDegrade PrimarySelfDegradeFunc

	// lastReceiptSent tracks when the last Receipt was sent to the
	// Primary, which should be updated atomically by the replication
	// layer. If zero, no Receipt has been sent yet.
	lastReceiptSent atomic.Int64

	// lastPrimaryNodeID tracks the Primary node ID from the previous
	// tick so we can clear lastReceiptSent on Primary changes.
	lastPrimaryMu     sync.Mutex
	lastPrimaryNodeID string

	electorHeartbeatSent atomic.Bool
	primaryHeartbeatSent atomic.Bool

	retryMetrics *metrics.RetryMetrics
}

// NewSender creates a heartbeat Sender.
func NewSender(
	logger *slog.Logger,
	nodeID string,
	state *nodestate.State,
	peers *peerclient.Manager,
	db RevisionSource,
	startTime int64,
	interval time.Duration,
	retryMetrics *metrics.RetryMetrics,
) *Sender {
	return &Sender{
		logger:       logger,
		nodeID:       nodeID,
		state:        state,
		peers:        peers,
		db:           db,
		startTime:    startTime,
		interval:     interval,
		retryMetrics: retryMetrics,
	}
}

// SetPrimarySelfDegradeFunc sets a callback invoked when this node is the
// Primary and self-degrades due to heartbeat failure. Must be called before
// Run.
func (s *Sender) SetPrimarySelfDegradeFunc(fn PrimarySelfDegradeFunc) {
	s.onPrimarySelfDegrade = fn
}

// MarkReceiptSent records that a Receipt was just sent to the Primary,
// resetting the standalone heartbeat timer. Called by the replication
// stream (Follow) layer each time a Receipt is sent.
func (s *Sender) MarkReceiptSent() {
	s.lastReceiptSent.Store(time.Now().UnixNano())
}

// Run starts the heartbeat sender loop. It blocks until ctx is
// cancelled.
func (s *Sender) Run(ctx context.Context) {
	if s.interval > 0 {
		s.runLoop(ctx)
	}
}

// runLoop sends heartbeats on a single cadence until the context is cancelled.
func (s *Sender) runLoop(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); s.sendToElector(ctx) }()
			go func() { defer wg.Done(); s.sendToPrimaryIfNeeded(ctx) }()
			wg.Wait()
		}
	}
}

// sendToElector sends one heartbeat to the current Elector, retrying once
// immediately before self-degrading this node.
func (s *Sender) sendToElector(ctx context.Context) {
	cs := s.state.ClusterState()
	client := s.peers.ElectorClient()

	if client == nil {
		return
	}

	ns := s.BuildNodeState()

	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := client.SendHeartbeat(sendCtx, ns); err != nil {
		s.logger.Warn("failed to send heartbeat",
			"target", "elector",
			"attempt", 1,
			"error", err,
		)

		if s.retryMetrics != nil {
			s.retryMetrics.Inc("heartbeat_send")
		}
		if _, err := client.SendHeartbeat(sendCtx, ns); err != nil {
			s.logger.Warn("failed to send heartbeat",
				"target", "elector",
				"attempt", 2,
				"error", err,
			)
			s.degradeSelf("elector heartbeat failed after retry", err)
			return
		}
	}

	if !s.electorHeartbeatSent.Swap(true) {
		s.logger.Info("first elector heartbeat sent", "target", cs.Elector.NodeID)
	}

	// When Elector == Primary, this heartbeat satisfies both since the
	// server-side processing uses the same code path. Mark it so the
	// primary loop does not send a redundant heartbeat.
	if cs.Elector.NodeID != "" && cs.Elector.NodeID == cs.Primary.NodeID {
		s.lastReceiptSent.Store(time.Now().UnixNano())
	}
}

// sendToPrimaryIfNeeded sends a standalone heartbeat to the Primary when the
// replication heartbeat cadence has elapsed without a recent Receipt.
func (s *Sender) sendToPrimaryIfNeeded(ctx context.Context) {
	cs := s.state.ClusterState()

	// Clear lastReceiptSent when the Primary changes so a receipt to
	// the old Primary does not suppress the first heartbeat to the new one.
	s.lastPrimaryMu.Lock()
	if cs.Primary.NodeID != s.lastPrimaryNodeID {
		s.lastPrimaryNodeID = cs.Primary.NodeID
		s.lastReceiptSent.Store(0)
	}
	s.lastPrimaryMu.Unlock()

	// Skip when Elector == Primary (covered by elector heartbeat),
	// or when this node is the Primary.
	sameNode := cs.Elector.NodeID != "" && cs.Elector.NodeID == cs.Primary.NodeID
	isPrimary := cs.Primary.NodeID != "" && cs.Primary.NodeID == s.nodeID

	if sameNode || isPrimary {
		return
	}

	client := s.peers.PrimaryClient()
	if client == nil {
		return
	}

	// Only send if no Receipt (or elector heartbeat covering primary)
	// was sent within the replication interval.
	lastReceipt := s.lastReceiptSent.Load()
	if lastReceipt > 0 {
		elapsed := time.Duration(time.Now().UnixNano() - lastReceipt)
		if elapsed < s.interval {
			return
		}
	}

	ns := s.BuildNodeState()

	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if _, err := client.SendHeartbeat(sendCtx, ns); err != nil {
		s.logger.Warn("failed to send heartbeat",
			"target", "primary",
			"attempt", 1,
			"error", err,
		)

		if s.retryMetrics != nil {
			s.retryMetrics.Inc("heartbeat_send")
		}
		if _, err := client.SendHeartbeat(sendCtx, ns); err != nil {
			s.logger.Warn("failed to send heartbeat",
				"target", "primary",
				"attempt", 2,
				"error", err,
			)
			s.degradeSelf("primary heartbeat failed after retry", err)
			return
		}
	}

	if !s.primaryHeartbeatSent.Swap(true) {
		s.logger.Info("first primary heartbeat sent", "target", cs.Primary.NodeID)
	}
}

// BuildNodeState constructs the heartbeat payload sent to the Elector or
// Primary, using the latest local revision when available.
func (s *Sender) BuildNodeState() *proto.NodeState {
	latestRevision, err := s.db.LatestRevision()
	if err != nil {
		s.logger.Warn("failed to get latest revision for heartbeat", "error", err)
	}

	return &proto.NodeState{
		NodeId:         s.nodeID,
		HealthState:    nodestate.HealthToProto(s.state.Health()),
		ElectorState:   nodestate.ElectorToProto(s.state.Elector()),
		PrimaryState:   nodestate.PrimaryToProto(s.state.Primary()),
		LatestRevision: latestRevision,
		StartTime:      s.startTime,
	}
}

// degradeSelf transitions this node to Degraded once and logs the cause.
// When this node is the Primary, the self-degradation callback is invoked
// to trigger the drain-flush-resign sequence.
func (s *Sender) degradeSelf(reason string, cause error) {
	if s.state.Health() == nodestate.HealthDegraded {
		return
	}

	wasPrimary := s.state.Primary() != nodestate.PrimaryReplica

	if err := s.state.SetHealth(nodestate.HealthDegraded); err != nil {
		s.logger.Warn("failed to self-degrade after heartbeat failure",
			"reason", reason,
			"cause", cause,
			"error", err,
		)
		return
	}

	s.logger.Warn("node self-degraded after heartbeat failure",
		"reason", reason,
		"cause", cause,
	)

	if wasPrimary && s.onPrimarySelfDegrade != nil {
		s.onPrimarySelfDegrade()
	}
}
