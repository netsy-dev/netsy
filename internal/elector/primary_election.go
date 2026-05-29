// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/netsy-dev/netsy/internal/nodestate"
)

// electionRetryInterval is how long the Elector waits between election
// attempts when no Primary is available.
const electionRetryInterval = 500 * time.Millisecond

// runPrimaryElectionLoop retries primary election every 500ms until a
// Primary is elected or ctx is cancelled. It is started as a goroutine
// when the node acquires Elector leadership.
func (s *Server) runPrimaryElectionLoop(ctx context.Context) {
	// Wait for bootstrap to complete.
	for {
		if s.nodeMap.Ready() {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !s.needsPrimaryElection() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(electionRetryInterval):
				continue
			}
		}

		s.logger.Info("election_started", "role", "elector", "registered_nodes", s.nodeMap.Count())
		electionStart := time.Now()
		elected, err := s.electPrimaryOnce(ctx)
		if err != nil {
			if s.metrics != nil {
				s.metrics.PrimaryElections.WithLabelValues("failure").Inc()
				s.metrics.PrimaryElectionDuration.WithLabelValues("failure").Observe(time.Since(electionStart).Seconds())
				s.metrics.PrimaryElectionFailures.WithLabelValues("contact_failure").Inc()
			}
			s.logger.Info("election_failed",
				"role", "elector",
				"reason", "contact_failure",
				"error", err,
				"duration_ms", time.Since(electionStart).Milliseconds(),
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(electionRetryInterval):
			}
			continue
		}

		s.storePreviousPrimary(nodestate.NodeInfo{})
		s.pushClusterState(ctx, elected)

		if s.metrics != nil {
			s.metrics.PrimaryElections.WithLabelValues("success").Inc()
			s.metrics.PrimaryElectionDuration.WithLabelValues("success").Observe(time.Since(electionStart).Seconds())
		}
		s.logger.Info("election_completed",
			"role", "elector",
			"elected_node_id", elected.NodeID,
			"member_id", elected.MemberID,
			"peer_addr", elected.PeerAdvertiseAddr,
			"registered_nodes", s.nodeMap.Count(),
			"duration_ms", time.Since(electionStart).Milliseconds(),
		)
	}
}

// needsPrimaryElection returns true if no Primary is currently set in
// the cluster state.
func (s *Server) needsPrimaryElection() bool {
	cs := s.state.ClusterState()
	return cs.Primary.NodeID == ""
}

// electPrimaryOnce attempts one round of primary election.
// It returns the elected NodeInfo or an error if election cannot proceed.
func (s *Server) electPrimaryOnce(ctx context.Context) (nodestate.NodeInfo, error) {
	// previous-Primary grace/drain check.
	if err := s.checkPreviousPrimary(ctx); err != nil {
		return nodestate.NodeInfo{}, fmt.Errorf("previous primary check: %w", err)
	}
	s.logger.Info("election_stage_completed", "role", "elector", "stage", "previous_primary_check")

	// collect node states per quorum rules.
	states, err := s.collectNodeStates(ctx)
	if err != nil {
		return nodestate.NodeInfo{}, fmt.Errorf("collect node states: %w", err)
	}
	s.logger.Info("election_stage_completed", "role", "elector", "stage", "collect_node_states", "contacted_nodes", len(states))

	// preserve existing Active Primary.
	if elected, ok := s.findActivePrimary(states); ok {
		return elected, nil
	}

	// fail if any non-degraded node reports non-Replica primary state.
	if err := s.checkNonReplicaStates(states); err != nil {
		return nodestate.NodeInfo{}, err
	}

	// filter to Healthy nodes.
	healthy := s.filterHealthy(states)
	if len(healthy) == 0 {
		return nodestate.NodeInfo{}, fmt.Errorf("no healthy candidates for primary election")
	}
	s.logger.Info("election_stage_completed", "role", "elector", "stage", "candidate_selection", "healthy_candidates", len(healthy))

	// tie-break by latest revision (desc), then start time (desc).
	sort.Slice(healthy, func(i, j int) bool {
		if healthy[i].latestRevision != healthy[j].latestRevision {
			return healthy[i].latestRevision > healthy[j].latestRevision
		}
		return healthy[i].startTime > healthy[j].startTime
	})

	winner := healthy[0]
	return winner.info, nil
}

// electionCandidate groups the data needed for primary election.
type electionCandidate struct {
	info           nodestate.NodeInfo
	healthState    nodestate.HealthState
	primaryState   nodestate.PrimaryState
	latestRevision int64
	startTime      int64
}

// checkPreviousPrimary contacts the previous Primary (if known) and
// waits for it to finish draining. If it is still Active, election is
// deferred. If unreachable within the configured timeout, election
// proceeds. It uses s.previousPrimary rather than ClusterState because
// ClusterState.Primary is cleared before election starts.
func (s *Server) checkPreviousPrimary(ctx context.Context) error {
	prevPrimary := s.loadPreviousPrimary()
	if prevPrimary.NodeID == "" {
		return nil
	}

	// When the previous Primary is this node, check local state
	// directly instead of making an RPC to self.
	if prevPrimary.NodeID == s.localNodeID {
		return s.checkLocalPreviousPrimary()
	}

	s.logger.Info("checking previous primary",
		"node_id", prevPrimary.NodeID,
		"addr", prevPrimary.PeerAdvertiseAddr,
	)

	timeout := s.primaryPriorTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}

	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		rpcCtx, rpcCancel := context.WithTimeout(ctx, 5*time.Second)
		state, err := s.peers.GetNodeState(rpcCtx, prevPrimary.PeerAdvertiseAddr)
		rpcCancel()
		if err != nil {
			s.logger.Debug("cannot reach previous primary, will retry",
				"node_id", prevPrimary.NodeID,
				"error", err,
			)
			if s.retryMetrics != nil {
				s.retryMetrics.Inc("election_contact")
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}

		ps := nodestate.PrimaryFromProto(state.GetPrimaryState())
		switch ps {
		case nodestate.PrimaryReplica:
			s.logger.Info("previous primary confirmed replica",
				"node_id", prevPrimary.NodeID,
			)
			s.storePreviousPrimary(nodestate.NodeInfo{})
			return nil
		case nodestate.PrimaryActive:
			return fmt.Errorf("previous primary %s is still active", prevPrimary.NodeID)
		case nodestate.PrimaryDraining:
			s.logger.Info("previous primary is draining, waiting",
				"node_id", prevPrimary.NodeID,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		case nodestate.PrimaryStarting:
			s.logger.Info("previous primary is starting, waiting",
				"node_id", prevPrimary.NodeID,
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
			continue
		}
	}

	s.logger.Warn("previous primary unreachable within timeout, proceeding with election",
		"node_id", prevPrimary.NodeID,
		"timeout", timeout,
	)
	s.storePreviousPrimary(nodestate.NodeInfo{})
	return nil
}

// checkLocalPreviousPrimary handles the case where this node was the
// previous Primary. It reads local Primary State directly instead of
// making an RPC to self.
func (s *Server) checkLocalPreviousPrimary() error {
	ps := s.state.Primary()
	s.logger.Info("checking local previous primary state",
		"primary_state", ps,
	)

	switch ps {
	case nodestate.PrimaryReplica:
		s.logger.Info("local node confirmed replica")
		s.storePreviousPrimary(nodestate.NodeInfo{})
		return nil
	case nodestate.PrimaryActive:
		return fmt.Errorf("local node is still active primary")
	case nodestate.PrimaryDraining:
		return fmt.Errorf("local node is still draining")
	case nodestate.PrimaryStarting:
		return fmt.Errorf("local node is in starting state")
	default:
		s.storePreviousPrimary(nodestate.NodeInfo{})
		return nil
	}
}

// collectNodeStates gathers node states from all registered nodes
// following the quorum-specific contactability rules.
func (s *Server) collectNodeStates(ctx context.Context) ([]electionCandidate, error) {
	entries := s.nodeMap.All()
	if len(entries) == 0 {
		return nil, fmt.Errorf("no registered nodes")
	}

	quorum := s.quorum

	// For disabled quorum (0), use existing heartbeat data from the
	// node map without contacting nodes.
	if quorum == 0 {
		return s.candidatesFromNodeMap(entries), nil
	}

	// Contact nodes in parallel.
	type result struct {
		candidate electionCandidate
		err       error
	}

	var wg sync.WaitGroup
	results := make([]result, len(entries))

	for i, entry := range entries {
		wg.Add(1)
		go func(idx int, e NodeEntry) {
			defer wg.Done()

			// Use local state for self.
			if e.NodeID == s.localNodeID {
				results[idx] = result{
					candidate: s.localCandidate(e),
				}
				return
			}

			rpcCtx, rpcCancel := context.WithTimeout(ctx, 5*time.Second)
			state, err := s.peers.GetNodeState(rpcCtx, e.PeerAdvertiseAddress)
			rpcCancel()
			if err != nil {
				results[idx] = result{err: fmt.Errorf("node %s: %w", e.NodeID, err)}
				return
			}

			results[idx] = result{
				candidate: electionCandidate{
					info: nodestate.NodeInfo{
						NodeID:            e.NodeID,
						MemberID:          e.MemberID,
						PeerAdvertiseAddr: e.PeerAdvertiseAddress,
					},
					healthState:    nodestate.HealthFromProto(state.GetHealthState()),
					primaryState:   nodestate.PrimaryFromProto(state.GetPrimaryState()),
					latestRevision: state.GetLatestRevision(),
					startTime:      state.GetStartTime(),
				},
			}
		}(i, entry)
	}
	wg.Wait()

	var candidates []electionCandidate
	var contactErrors []error
	contacted := 0

	for _, r := range results {
		if r.err != nil {
			contactErrors = append(contactErrors, r.err)
			continue
		}
		contacted++
		candidates = append(candidates, r.candidate)
	}

	if s.metrics != nil {
		if contacted > 0 {
			s.metrics.PrimaryElectionContacts.WithLabelValues("success").Add(float64(contacted))
		}
		if len(contactErrors) > 0 {
			s.metrics.PrimaryElectionContacts.WithLabelValues("failure").Add(float64(len(contactErrors)))
		}
	}

	// Enforce contactability rules.
	totalNodes := len(entries)

	switch {
	case quorum > 0:
		// Static quorum: all registered nodes must be contacted.
		if contacted < totalNodes {
			return nil, fmt.Errorf("static quorum requires all %d nodes, only contacted %d: %v",
				totalNodes, contacted, contactErrors)
		}
	case quorum == -1:
		// Majority quorum: floor(N/2) + 1 must be contacted.
		majority := (totalNodes / 2) + 1
		if contacted < majority {
			return nil, fmt.Errorf("majority quorum requires %d of %d nodes, only contacted %d: %v",
				majority, totalNodes, contacted, contactErrors)
		}
	}

	return candidates, nil
}

// candidatesFromNodeMap builds election candidates from the node map's
// heartbeat data. Used when quorum is disabled (0).
func (s *Server) candidatesFromNodeMap(entries []NodeEntry) []electionCandidate {
	candidates := make([]electionCandidate, 0, len(entries))
	for _, e := range entries {
		candidates = append(candidates, electionCandidate{
			info: nodestate.NodeInfo{
				NodeID:            e.NodeID,
				MemberID:          e.MemberID,
				PeerAdvertiseAddr: e.PeerAdvertiseAddress,
			},
			healthState:    e.HealthState,
			primaryState:   e.PrimaryState,
			latestRevision: e.LatestRevision,
			startTime:      e.StartTime,
		})
	}
	return candidates
}

// localCandidate builds an election candidate for the local node using
// the node map entry and local state.
func (s *Server) localCandidate(e NodeEntry) electionCandidate {
	latestRevision := e.LatestRevision
	if s.localDB != nil {
		if rev, err := s.localDB.LatestRevision(); err == nil {
			latestRevision = rev
		}
	}

	return electionCandidate{
		info: nodestate.NodeInfo{
			NodeID:            e.NodeID,
			MemberID:          e.MemberID,
			PeerAdvertiseAddr: e.PeerAdvertiseAddress,
		},
		healthState:    s.state.Health(),
		primaryState:   s.state.Primary(),
		latestRevision: latestRevision,
		startTime:      s.localStartTime,
	}
}

// findActivePrimary checks if exactly one contacted node reports Active
// primary state. If so, it returns that node as the elected Primary.
func (s *Server) findActivePrimary(candidates []electionCandidate) (nodestate.NodeInfo, bool) {
	var active []electionCandidate
	for _, c := range candidates {
		if c.primaryState == nodestate.PrimaryActive {
			active = append(active, c)
		}
	}

	if len(active) == 1 {
		s.logger.Info("preserving existing active primary",
			"node_id", active[0].info.NodeID,
		)
		return active[0].info, true
	}

	if len(active) > 1 {
		s.logger.Error("multiple nodes report active primary state",
			"count", len(active),
		)
	}

	return nodestate.NodeInfo{}, false
}

// checkNonReplicaStates fails election if any contacted non-degraded
// node reports a primary state other than Replica. Degraded nodes are
// excluded from this check.
func (s *Server) checkNonReplicaStates(candidates []electionCandidate) error {
	for _, c := range candidates {
		if c.healthState == nodestate.HealthDegraded {
			continue
		}
		if c.primaryState != nodestate.PrimaryReplica {
			return fmt.Errorf("non-degraded node %s has primary state %s",
				c.info.NodeID, c.primaryState)
		}
	}
	return nil
}

// filterHealthy returns only candidates with Healthy health state.
func (s *Server) filterHealthy(candidates []electionCandidate) []electionCandidate {
	var healthy []electionCandidate
	for _, c := range candidates {
		if c.healthState == nodestate.HealthHealthy {
			healthy = append(healthy, c)
		}
	}
	return healthy
}
