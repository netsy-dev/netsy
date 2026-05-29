// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"log/slog"
	"testing"
	"time"
)

func TestElectorGuardFirstCheck(t *testing.T) {
	g := NewElectorGuard(slog.Default())
	if err := g.Check("elector-a"); err != nil {
		t.Fatalf("first check failed: %v", err)
	}
	if g.currentElector != "elector-a" {
		t.Fatalf("expected currentElector=elector-a, got %s", g.currentElector)
	}
}

func TestElectorGuardSameElector(t *testing.T) {
	g := NewElectorGuard(slog.Default())
	_ = g.Check("elector-a")
	if err := g.Check("elector-a"); err != nil {
		t.Fatalf("same elector check failed: %v", err)
	}
}

func TestElectorGuardNormalHandoff(t *testing.T) {
	g := NewElectorGuard(slog.Default())
	_ = g.Check("elector-a")

	// A → B is a normal handoff.
	if err := g.Check("elector-b"); err != nil {
		t.Fatalf("handoff check failed: %v", err)
	}
	if g.currentElector != "elector-b" {
		t.Fatalf("expected currentElector=elector-b, got %s", g.currentElector)
	}
	if g.previousElector != "elector-a" {
		t.Fatalf("expected previousElector=elector-a, got %s", g.previousElector)
	}
}

func TestElectorGuardSplitBrain(t *testing.T) {
	g := NewElectorGuard(slog.Default())

	// A pushes, then B pushes (handoff), then A pushes again
	// immediately — both are concurrently active.
	_ = g.Check("elector-a")
	_ = g.Check("elector-b")

	if err := g.Check("elector-a"); err == nil {
		t.Fatal("expected error on split-brain detection")
	}
	// The guard should reject the returning elector and keep the current one.
	if g.currentElector != "elector-b" {
		t.Fatalf("expected currentElector=elector-b (unchanged), got %s", g.currentElector)
	}
}

func TestElectorGuardReElectionAfterWindow(t *testing.T) {
	g := NewElectorGuard(slog.Default())

	// A → B handoff.
	_ = g.Check("elector-a")
	_ = g.Check("elector-b")

	// Simulate time passing beyond the split-brain window.
	g.mu.Lock()
	g.handoffAt = time.Now().Add(-splitBrainWindow - time.Second)
	g.mu.Unlock()

	// A returns after the window — this is a normal re-election.
	if err := g.Check("elector-a"); err != nil {
		t.Fatalf("expected normal re-election after window, got error: %v", err)
	}
	if g.currentElector != "elector-a" {
		t.Fatalf("expected currentElector=elector-a, got %s", g.currentElector)
	}
}

func TestElectorGuardThreeWayHandoff(t *testing.T) {
	g := NewElectorGuard(slog.Default())

	// A → B → C is sequential handoffs, not split-brain.
	_ = g.Check("elector-a")
	_ = g.Check("elector-b")
	_ = g.Check("elector-c")

	if g.currentElector != "elector-c" {
		t.Fatalf("expected currentElector=elector-c, got %s", g.currentElector)
	}
	if g.previousElector != "elector-b" {
		t.Fatalf("expected previousElector=elector-b, got %s", g.previousElector)
	}
}
