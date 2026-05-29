// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"context"
	"time"

	"github.com/netsy-dev/netsy/internal/discovery"
)

// compactionNoticeTimeout is the per-node timeout for sending a
// compaction notice and waiting for a response.
const compactionNoticeTimeout = 5 * time.Second

// RunCompactionScheduler runs a periodic loop that queries all registered
// nodes for their minimum watch revision, computes the cluster-wide
// global minimum, and drives the compaction notice/confirmation protocol.
func (s *Server) RunCompactionScheduler(ctx context.Context) {
	interval := s.config.CompactionInterval.Duration
	if interval <= 0 {
		s.logger.Info("compaction scheduling disabled (compaction_interval not set)")
		return
	}

	s.logger.Info("compaction scheduler started", "interval", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("compaction scheduler stopping")
			return
		case <-ticker.C:
			s.runCompactionCycle(ctx)
		}
	}
}

// runCompactionCycle performs a single compaction scheduling pass. It
// queries every registered node for its minimum watch revision, computes
// the global minimum, and if the revision has advanced, runs the
// compaction notice/confirmation protocol.
func (s *Server) runCompactionCycle(ctx context.Context) {
	if err := s.requireActivePrimary(); err != nil {
		return
	}

	registrations, err := discovery.ListNodeRegistrations(ctx, s.storageClient)
	if err != nil {
		s.logger.Warn("compaction cycle: failed to list node registrations", "error", err)
		return
	}

	if len(registrations) == 0 {
		s.logger.Debug("compaction cycle: no registered nodes found")
		return
	}

	var globalMin int64 = -1

	for _, reg := range registrations {
		addr := reg.PeerAdvertiseAddress
		if addr == "" {
			s.logger.Warn("compaction cycle: node has no peer advertise address",
				"node_id", reg.NodeID,
			)
			return
		}

		queryCtx, cancel := context.WithTimeout(ctx, compactionNoticeTimeout)
		minRev, err := s.peerClients.GetMinWatchRevision(queryCtx, addr)
		cancel()

		if err != nil {
			s.logger.Warn("compaction cycle: failed to query node, aborting cycle",
				"node_id", reg.NodeID,
				"error", err,
			)
			return
		}

		if globalMin < 0 || minRev < globalMin {
			globalMin = minRev
		}
	}

	if globalMin <= 0 {
		s.logger.Debug("compaction cycle: global minimum revision is zero, skipping")
		return
	}

	// The compaction revision is inclusive (matching etcd semantics),
	// so we compact up to globalMin-1 to preserve the revision that
	// the lowest active watch is tracking.
	proposedRevision := globalMin - 1
	if proposedRevision <= 0 {
		s.logger.Debug("compaction cycle: proposed revision is zero after adjustment, skipping")
		return
	}

	currentCompaction := s.state.Compaction()
	if proposedRevision <= currentCompaction {
		s.logger.Debug("compaction cycle: no advancement needed",
			"global_min", globalMin,
			"current_compaction", currentCompaction,
		)
		return
	}

	s.logger.Info("compaction_started",
		"proposed_revision", proposedRevision,
		"global_min_watch", globalMin,
		"current_compaction", currentCompaction,
		"nodes_queried", len(registrations),
	)

	s.runCompactionProtocol(ctx, registrations, proposedRevision)
}

// runCompactionProtocol sends compaction notices to all Nodes, retrying
// each once on failure. If any Node fails to confirm after retry, the
// protocol aborts and rolls back all already-confirmed Nodes by sending a
// new notice with the previous compaction revision. On cluster-wide
// acceptance, it persists the revision locally, broadcasts the compact
// message on Follow streams, and enqueues async compaction execution.
func (s *Server) runCompactionProtocol(ctx context.Context, registrations []discovery.NodeRegistration, compactionRevision int64) {
	compactionStart := time.Now()
	previousCompaction := s.state.Compaction()
	confirmedAddrs := make([]string, 0, len(registrations))

	for _, reg := range registrations {
		var confirmed bool
		for attempt := 1; attempt <= 2; attempt++ {
			if attempt > 1 && s.retryMetrics != nil {
				s.retryMetrics.Inc("compaction_confirmation")
			}
			noticeCtx, cancel := context.WithTimeout(ctx, compactionNoticeTimeout)
			ok, err := s.peerClients.SendCompactionNotice(noticeCtx, reg.PeerAdvertiseAddress, compactionRevision)
			cancel()

			if err != nil {
				s.logger.Warn("compaction_notice_failed",
					"node_id", reg.NodeID, "attempt", attempt, "error", err,
				)
				continue
			}
			if !ok {
				s.logger.Warn("compaction_notice_failed",
					"node_id", reg.NodeID, "attempt", attempt, "reason", "rejected",
				)
				continue
			}
			confirmed = true
			break
		}
		if !confirmed {
			s.logger.Warn("compaction cycle: aborting, node failed to confirm after retry",
				"node_id", reg.NodeID,
				"compaction_revision", compactionRevision,
			)
			if s.metrics != nil {
				s.metrics.CompactionsTotal.WithLabelValues("error").Inc()
				s.metrics.CompactionCoordDuration.WithLabelValues("error").Observe(time.Since(compactionStart).Seconds())
				s.metrics.CompactionConfirmFailures.WithLabelValues("node_rejected").Inc()
			}
			for _, addr := range confirmedAddrs {
				rollbackCtx, cancel := context.WithTimeout(ctx, compactionNoticeTimeout)
				_, err := s.peerClients.SendCompactionNotice(rollbackCtx, addr, previousCompaction)
				cancel()
				if err != nil {
					s.logger.Warn("compaction rollback: failed to reset node floor",
						"addr", addr, "error", err,
					)
				}
			}
			return
		}
		confirmedAddrs = append(confirmedAddrs, reg.PeerAdvertiseAddress)
	}

	// All nodes confirmed — persist and broadcast.
	if err := s.db.PersistCompactionRevision(compactionRevision); err != nil {
		s.logger.Error("compaction cycle: failed to persist compaction revision locally",
			"compaction_revision", compactionRevision,
			"error", err,
		)
		return
	}

	s.state.SetCompaction(compactionRevision)
	s.BroadcastCompact(compactionRevision)

	if s.metrics != nil {
		s.metrics.CompactionsTotal.WithLabelValues("success").Inc()
		s.metrics.CompactionCoordDuration.WithLabelValues("success").Observe(time.Since(compactionStart).Seconds())
	}
	s.logger.Info("compaction_completed",
		"compaction_revision", compactionRevision,
		"duration_ms", time.Since(compactionStart).Milliseconds(),
	)

	go s.executeCompaction(compactionRevision)
}

// executeCompaction runs the actual data compaction asynchronously.
// SQLite WAL mode provides snapshot isolation, so concurrent snapshot
// reads are safe without additional locking.
func (s *Server) executeCompaction(compactionRevision int64) {
	start := time.Now()
	affected, err := s.db.ExecuteCompaction(compactionRevision)
	if err != nil {
		if s.compactionMetrics != nil {
			s.compactionMetrics.Duration.WithLabelValues("error").Observe(time.Since(start).Seconds())
		}
		s.logger.Error("compaction execution failed",
			"compaction_revision", compactionRevision,
			"error", err,
		)
		return
	}

	if s.compactionMetrics != nil {
		s.compactionMetrics.Duration.WithLabelValues("success").Observe(time.Since(start).Seconds())
	}
	s.logger.Info("compaction execution completed",
		"compaction_revision", compactionRevision,
		"records_compacted", affected,
	)
}
