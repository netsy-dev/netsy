// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"context"
	"sync"
	"time"

	"github.com/netsy-dev/netsy/internal/nodestate"
)

// pushTimeout is the RPC timeout for pushing cluster state to a single node.
const pushTimeout = 5 * time.Second

// pushClusterState builds the current authoritative cluster state with the
// given Primary and distributes it. The local node's state is updated first,
// then the Primary node, then all remaining nodes in parallel. Failures are
// logged but do not block subsequent pushes.
//
// The caller provides the desired Primary so that the shared state is only
// updated inside ApplyClusterState, which ensures that the role-change
// callback fires correctly (it compares old vs new Primary).
func (s *Server) pushClusterState(ctx context.Context, primary nodestate.NodeInfo) {
	cs := s.state.ClusterState()
	cs.Primary = primary
	cs.NodeCount = s.nodeMap.Count()

	// Apply cluster state to this node first. This must happen even
	// when no remote nodes are registered (e.g. single-node clusters
	// or after deregistration clears the map).
	s.peers.ApplyClusterState(ctx, cs)

	// Return early if there's no remote nodes to push to.
	entries := s.nodeMap.All()
	if len(entries) <= 1 {
		return
	}

	s.logger.Info("pushing cluster state",
		"elector", cs.Elector.NodeID,
		"primary", cs.Primary.NodeID,
		"node_count", len(entries),
	)

	// Remove self from the list.
	remote := make([]NodeEntry, 0, len(entries)-1)
	for _, e := range entries {
		if e.NodeID != s.localNodeID {
			remote = append(remote, e)
		}
	}

	protoCS := nodestate.ClusterStateToProto(cs)
	push := func(nodeID, addr string) {
		pushCtx, cancel := context.WithTimeout(ctx, pushTimeout)
		defer cancel()

		if err := s.peers.PushClusterStateTo(pushCtx, addr, protoCS); err != nil {
			s.logger.Warn("failed to push cluster state to node",
				"node_id", nodeID,
				"addr", addr,
				"error", err,
			)
			return
		}
		s.logger.Debug("pushed cluster state to node", "node_id", nodeID)
	}

	// Push to the Primary first synchronously.
	for _, e := range remote {
		if e.NodeID == cs.Primary.NodeID {
			push(e.NodeID, e.PeerAdvertiseAddress)
			break
		}
	}

	// Push to all remaining nodes in parallel.
	var wg sync.WaitGroup
	for _, e := range remote {
		if e.NodeID == cs.Primary.NodeID {
			continue
		}
		wg.Add(1)
		go func(nodeID, addr string) {
			defer wg.Done()
			push(nodeID, addr)
		}(e.NodeID, e.PeerAdvertiseAddress)
	}
	wg.Wait()
}
