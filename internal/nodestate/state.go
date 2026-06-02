// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package nodestate

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
)

// State holds the current node state triple and enforces valid transitions.
type State struct {
	mu      sync.RWMutex
	logger  *slog.Logger
	metrics *StateMetrics
	health  HealthState
	elector ElectorState
	primary PrimaryState
	cluster ClusterState

	// committed and compaction use atomics for lock-free access because they
	// are read on the hot path for Range and watch delivery.
	committed  atomic.Int64
	compaction atomic.Int64

	// memberID is learned once during bootstrap and then treated as immutable.
	memberID uint64
}

// New returns a State initialised to Loading / Follower / Replica.
func New(logger *slog.Logger) *State {
	return &State{
		logger:  logger,
		health:  HealthLoading,
		elector: ElectorFollower,
		primary: PrimaryReplica,
	}
}

// SetMetrics sets the Prometheus state metrics that are updated on
// state transitions. Must be called before any concurrent state changes.
func (s *State) SetMetrics(m *StateMetrics) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = m
}

// Health returns the current HealthState.
func (s *State) Health() HealthState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.health
}

// Elector returns the current ElectorState.
func (s *State) Elector() ElectorState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.elector
}

// Primary returns the current PrimaryState.
func (s *State) Primary() PrimaryState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.primary
}

// SetHealth transitions the HealthState. It returns an error if the
// transition is not valid.
func (s *State) SetHealth(to HealthState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	from := s.health
	if !validHealthTransition(from, to) {
		return fmt.Errorf("invalid health transition: %s -> %s", from, to)
	}
	s.health = to
	s.logger.Info("state_transition",
		"state_type", "health",
		"previous", string(from),
		"new", string(to),
	)
	if s.metrics != nil {
		s.metrics.SetHealth(string(to))
	}
	return nil
}

// SetElector transitions the ElectorState. It returns an error if the
// transition is not valid.
func (s *State) SetElector(to ElectorState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	from := s.elector
	if !validElectorTransition(from, to) {
		return fmt.Errorf("invalid elector transition: %s -> %s", from, to)
	}
	s.elector = to
	s.logger.Info("state_transition",
		"state_type", "elector",
		"previous", string(from),
		"new", string(to),
	)
	if s.metrics != nil {
		s.metrics.SetElector(string(to))
	}
	return nil
}

// SetPrimary transitions the PrimaryState. It returns an error if the
// transition is not valid.
func (s *State) SetPrimary(to PrimaryState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	from := s.primary
	if !validPrimaryTransition(from, to) {
		return fmt.Errorf("invalid primary transition: %s -> %s", from, to)
	}
	s.primary = to
	s.logger.Info("state_transition",
		"state_type", "primary",
		"previous", string(from),
		"new", string(to),
	)
	if s.metrics != nil {
		s.metrics.SetPrimary(string(to))
	}
	return nil
}

// Committed returns the current committed revision.
func (s *State) Committed() int64 {
	return s.committed.Load()
}

// SetCommitted sets the committed revision.
func (s *State) SetCommitted(rev int64) {
	s.committed.Store(rev)
	if s.metrics != nil {
		s.metrics.CommittedRevision.Set(float64(rev))
	}
}

// Compaction returns the current compaction revision.
func (s *State) Compaction() int64 {
	return s.compaction.Load()
}

// SetCompaction sets the compaction revision.
func (s *State) SetCompaction(rev int64) {
	s.compaction.Store(rev)
	if s.metrics != nil {
		s.metrics.CompactionRevision.Set(float64(rev))
	}
}

// SetLatest sets the latest revision gauge for Prometheus scraping.
func (s *State) SetLatest(rev int64) {
	if s.metrics != nil {
		s.metrics.LatestRevision.Set(float64(rev))
	}
}

// MemberID returns the local stable etcd member ID.
func (s *State) MemberID() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.memberID
}

// SetMemberID records the local stable etcd member ID once. Repeating the
// same assignment is allowed; attempting to change an existing member ID is an error.
func (s *State) SetMemberID(memberID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.memberID == 0 {
		s.memberID = memberID
		return nil
	}
	if s.memberID != memberID {
		return fmt.Errorf("member_id already set to %d", s.memberID)
	}
	return nil
}
