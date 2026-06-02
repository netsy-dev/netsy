// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"context"
	"time"

	"github.com/netsy-dev/netsy/internal/discovery"
	"github.com/netsy-dev/netsy/internal/nodestate"
)

// healthCheckInterval is how often the Elector checks node health. This
// single loop handles both heartbeat-based degradation and
// auto-deregistration of prolonged degraded nodes.
const healthCheckInterval = 500 * time.Millisecond

// runHealthCheckLoop periodically iterates all nodes in the node map,
// marking any node as Degraded if it has missed heartbeats and
// auto-deregistering nodes that have been degraded beyond the
// deregistration timeout.
func (s *Server) runHealthCheckLoop(ctx context.Context) {
	if s.heartbeatInterval == 0 {
		s.logger.Warn("heartbeat interval is 0, health checking disabled")
		return
	}

	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkNodeHealth(ctx)
		}
	}
}

// checkNodeHealth iterates all nodes once, performing two checks per
// node: degradation (missed heartbeats) and deregistration (prolonged
// degradation). It collects node IDs to act on during iteration, then
// applies mutations after releasing the read lock to avoid deadlock.
//
// When the current Primary is degraded or deregistered, the previous
// Primary identity is saved for election step 1 (drain check),
// ClusterState.Primary is cleared, and the updated state is pushed to
// all nodes so they disconnect from the old Primary.
func (s *Server) checkNodeHealth(ctx context.Context) {
	if !s.nodeMap.Ready() {
		return
	}

	now := time.Now()
	degradationDeadline := time.Duration(s.degradationCount) * s.heartbeatInterval

	var toDegraded []string
	var toDeregistered []string

	s.nodeMap.ForEach(func(entry NodeEntry) {
		if entry.HealthState != nodestate.HealthDegraded {
			if now.Sub(entry.LastHeartbeat) >= degradationDeadline {
				toDegraded = append(toDegraded, entry.NodeID)
			}
			return
		}

		if s.deregTimeout > 0 && !entry.DegradedAt.IsZero() && now.Sub(entry.DegradedAt) >= s.deregTimeout {
			toDeregistered = append(toDeregistered, entry.NodeID)
		}
	})

	for _, nodeID := range toDegraded {
		s.logger.Warn("marking node degraded due to missed heartbeats",
			"node_id", nodeID,
			"deadline", degradationDeadline,
		)
		s.nodeMap.SetHealthState(nodeID, nodestate.HealthDegraded)
	}

	for _, nodeID := range toDeregistered {
		var memberID uint64
		if entry, ok := s.nodeMap.Get(nodeID); ok {
			memberID = entry.MemberID
		}
		s.logger.Info("node_deregistered",
			"target_node_id", nodeID,
			"member_id", memberID,
			"trigger", "auto",
			"reason", "timeout",
		)
		s.nodeMap.Remove(nodeID)
		if s.metrics != nil {
			s.metrics.AutoDeregistrations.Inc()
		}

		if err := discovery.DeleteNodeRegistration(ctx, s.store, nodeID); err != nil {
			s.logger.Warn("failed to delete registration file during auto-deregistration",
				"node_id", nodeID,
				"error", err,
			)
		}
	}

	// Update node health gauges after all mutations.
	if s.metrics != nil {
		var registered, healthy, degraded int
		s.nodeMap.ForEach(func(entry NodeEntry) {
			registered++
			switch entry.HealthState {
			case nodestate.HealthHealthy:
				healthy++
			case nodestate.HealthDegraded:
				degraded++
			}
		})
		s.metrics.RegisteredNodes.Set(float64(registered))
		s.metrics.HealthyNodes.Set(float64(healthy))
		s.metrics.DegradedNodes.Set(float64(degraded))
	}

	// If the current Primary was degraded or deregistered, clear it
	// from ClusterState so the election loop triggers re-election.
	primaryNodeID := s.state.ClusterState().Primary.NodeID
	if primaryNodeID == "" {
		return
	}
	for _, nodeID := range toDegraded {
		if nodeID == primaryNodeID {
			s.clearPrimary(ctx, "primary degraded due to missed heartbeats")
			return
		}
	}
	for _, nodeID := range toDeregistered {
		if nodeID == primaryNodeID {
			s.clearPrimary(ctx, "primary auto-deregistered")
			return
		}
	}
}

// clearPrimary saves the current Primary as previousPrimary for the
// election drain check, clears ClusterState.Primary, and pushes the
// updated state to all nodes.
func (s *Server) clearPrimary(ctx context.Context, reason string) {
	cs := s.state.ClusterState()
	s.logger.Warn("clearing primary from cluster state",
		"node_id", cs.Primary.NodeID,
		"reason", reason,
	)
	s.storePreviousPrimary(cs.Primary)
	s.pushClusterState(ctx, nodestate.NodeInfo{})
}
