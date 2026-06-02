// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package nodestate

import (
	"log/slog"
	"testing"
)

func newTestState() *State {
	return New(slog.Default())
}

func TestInitialState(t *testing.T) {
	s := newTestState()
	if s.Health() != HealthLoading {
		t.Fatalf("expected HealthLoading, got %s", s.Health())
	}
	if s.Elector() != ElectorFollower {
		t.Fatalf("expected ElectorFollower, got %s", s.Elector())
	}
	if s.Primary() != PrimaryReplica {
		t.Fatalf("expected PrimaryReplica, got %s", s.Primary())
	}
}

func TestHealthTransitions(t *testing.T) {
	tests := []struct {
		name    string
		from    HealthState
		to      HealthState
		wantErr bool
	}{
		{"loading->healthy", HealthLoading, HealthHealthy, false},
		{"healthy->degraded", HealthHealthy, HealthDegraded, false},
		{"degraded->loading", HealthDegraded, HealthLoading, false},
		{"loading->degraded", HealthLoading, HealthDegraded, false},
		{"healthy->loading", HealthHealthy, HealthLoading, true},
		{"degraded->healthy", HealthDegraded, HealthHealthy, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestState()
			// Walk state to the desired starting point.
			switch tt.from {
			case HealthHealthy:
				_ = s.SetHealth(HealthHealthy)
			case HealthDegraded:
				_ = s.SetHealth(HealthHealthy)
				_ = s.SetHealth(HealthDegraded)
			}
			err := s.SetHealth(tt.to)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SetHealth(%s -> %s) error = %v, wantErr %v", tt.from, tt.to, err, tt.wantErr)
			}
			if err == nil && s.Health() != tt.to {
				t.Fatalf("expected %s, got %s", tt.to, s.Health())
			}
		})
	}
}

func TestElectorTransitions(t *testing.T) {
	tests := []struct {
		name    string
		from    ElectorState
		to      ElectorState
		wantErr bool
	}{
		{"follower->leader", ElectorFollower, ElectorLeader, false},
		{"leader->follower", ElectorLeader, ElectorFollower, false},
		{"follower->follower", ElectorFollower, ElectorFollower, true},
		{"leader->leader", ElectorLeader, ElectorLeader, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestState()
			if tt.from == ElectorLeader {
				_ = s.SetElector(ElectorLeader)
			}
			err := s.SetElector(tt.to)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SetElector(%s -> %s) error = %v, wantErr %v", tt.from, tt.to, err, tt.wantErr)
			}
			if err == nil && s.Elector() != tt.to {
				t.Fatalf("expected %s, got %s", tt.to, s.Elector())
			}
		})
	}
}

func TestPrimaryTransitions(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*State)
		to      PrimaryState
		wantErr bool
	}{
		{"replica->starting", func(s *State) {}, PrimaryStarting, false},
		{"starting->active", func(s *State) { _ = s.SetPrimary(PrimaryStarting) }, PrimaryActive, false},
		{"active->draining", func(s *State) {
			_ = s.SetPrimary(PrimaryStarting)
			_ = s.SetPrimary(PrimaryActive)
		}, PrimaryDraining, false},
		{"draining->replica", func(s *State) {
			_ = s.SetPrimary(PrimaryStarting)
			_ = s.SetPrimary(PrimaryActive)
			_ = s.SetPrimary(PrimaryDraining)
		}, PrimaryReplica, false},
		{"replica->active", func(s *State) {}, PrimaryActive, true},
		{"replica->draining", func(s *State) {}, PrimaryDraining, true},
		{"starting->replica", func(s *State) { _ = s.SetPrimary(PrimaryStarting) }, PrimaryReplica, false},
		{"starting->draining", func(s *State) { _ = s.SetPrimary(PrimaryStarting) }, PrimaryDraining, false},
		{"active->replica", func(s *State) {
			_ = s.SetPrimary(PrimaryStarting)
			_ = s.SetPrimary(PrimaryActive)
		}, PrimaryReplica, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newTestState()
			tt.setup(s)
			err := s.SetPrimary(tt.to)
			if (err != nil) != tt.wantErr {
				t.Fatalf("SetPrimary -> %s error = %v, wantErr %v", tt.to, err, tt.wantErr)
			}
		})
	}
}

func TestCommittedRevision(t *testing.T) {
	s := newTestState()
	if s.Committed() != 0 {
		t.Fatalf("expected initial committed 0, got %d", s.Committed())
	}
	s.SetCommitted(42)
	if s.Committed() != 42 {
		t.Fatalf("expected committed 42, got %d", s.Committed())
	}
}

func TestCompactionRevision(t *testing.T) {
	s := newTestState()
	if s.Compaction() != 0 {
		t.Fatalf("expected initial compaction 0, got %d", s.Compaction())
	}
	s.SetCompaction(10)
	if s.Compaction() != 10 {
		t.Fatalf("expected compaction 10, got %d", s.Compaction())
	}
}

func TestMemberIDWriteOnce(t *testing.T) {
	s := newTestState()

	if got := s.MemberID(); got != 0 {
		t.Fatalf("initial MemberID() = %d, want 0", got)
	}

	if err := s.SetMemberID(7); err != nil {
		t.Fatalf("SetMemberID(7) error = %v", err)
	}
	if got := s.MemberID(); got != 7 {
		t.Fatalf("MemberID() = %d, want 7", got)
	}

	if err := s.SetMemberID(7); err != nil {
		t.Fatalf("SetMemberID(7) second time error = %v", err)
	}

	if err := s.SetMemberID(8); err == nil {
		t.Fatal("SetMemberID(8) succeeded, want error")
	}
}
