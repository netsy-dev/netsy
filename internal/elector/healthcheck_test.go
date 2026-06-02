// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/peerclient"
	"github.com/netsy-dev/netsy/internal/storage"
)

func newTestServer(heartbeatInterval time.Duration, degradationCount int) *Server {
	state := nodestate.New(slog.Default())
	_ = state.SetElector(nodestate.ElectorLeader)
	mgr := peerclient.NewManager(slog.Default(), "test-node", nil, state)
	return NewServer(
		slog.Default(),
		"test-cluster",
		storage.NewMemoryStore(),
		state,
		heartbeatInterval,
		0, // deregTimeout
		degradationCount,
		"test-node", 0, nil, 0, 0, mgr, nil, nil,
	)
}

func TestCheckNodeHealthMarksStaleNode(t *testing.T) {
	srv := newTestServer(50*time.Millisecond, 2)
	srv.nodeMap.SetReady()

	srv.nodeMap.Add(NodeEntry{
		NodeID:        "node-a",
		MemberID:      1,
		LastHeartbeat: time.Now().Add(-200 * time.Millisecond),
		HealthState:   nodestate.HealthHealthy,
	})

	srv.checkNodeHealth(context.Background())

	entry, ok := srv.nodeMap.Get("node-a")
	if !ok {
		t.Fatal("expected node-a to exist")
	}
	if entry.HealthState != nodestate.HealthDegraded {
		t.Fatalf("expected degraded, got %s", entry.HealthState)
	}
}

func TestCheckNodeHealthSkipsFreshNode(t *testing.T) {
	srv := newTestServer(50*time.Millisecond, 2)
	srv.nodeMap.SetReady()

	srv.nodeMap.Add(NodeEntry{
		NodeID:        "node-a",
		MemberID:      1,
		LastHeartbeat: time.Now(),
		HealthState:   nodestate.HealthHealthy,
	})

	srv.checkNodeHealth(context.Background())

	entry, ok := srv.nodeMap.Get("node-a")
	if !ok {
		t.Fatal("expected node-a to exist")
	}
	if entry.HealthState != nodestate.HealthHealthy {
		t.Fatalf("expected healthy, got %s", entry.HealthState)
	}
}

func TestCheckNodeHealthSkipsAlreadyDegraded(t *testing.T) {
	srv := newTestServer(50*time.Millisecond, 2)
	srv.nodeMap.SetReady()

	srv.nodeMap.Add(NodeEntry{
		NodeID:        "node-a",
		MemberID:      1,
		LastHeartbeat: time.Now().Add(-time.Hour),
		HealthState:   nodestate.HealthDegraded,
	})

	srv.checkNodeHealth(context.Background())

	entry, ok := srv.nodeMap.Get("node-a")
	if !ok {
		t.Fatal("expected node-a to exist")
	}
	if entry.HealthState != nodestate.HealthDegraded {
		t.Fatalf("expected degraded, got %s", entry.HealthState)
	}
}

func TestCheckNodeHealthNotReadySkips(t *testing.T) {
	srv := newTestServer(50*time.Millisecond, 2)

	srv.nodeMap.Add(NodeEntry{
		NodeID:        "node-a",
		MemberID:      1,
		LastHeartbeat: time.Now().Add(-time.Hour),
		HealthState:   nodestate.HealthHealthy,
	})

	srv.checkNodeHealth(context.Background())

	entry, ok := srv.nodeMap.Get("node-a")
	if !ok {
		t.Fatal("expected node-a to exist")
	}
	if entry.HealthState != nodestate.HealthHealthy {
		t.Fatalf("expected healthy (not ready, should skip), got %s", entry.HealthState)
	}
}

func TestCheckNodeHealthDeregistersAfterTimeout(t *testing.T) {
	state := nodestate.New(slog.Default())
	_ = state.SetElector(nodestate.ElectorLeader)
	mgr := peerclient.NewManager(slog.Default(), "test-node", nil, state)
	srv := NewServer(
		slog.Default(),
		"test-cluster",
		storage.NewMemoryStore(),
		state,
		50*time.Millisecond,
		100*time.Millisecond, // deregTimeout
		2,
		"test-node", 0, nil, 0, 0, mgr, nil, nil,
	)
	srv.nodeMap.SetReady()

	srv.nodeMap.Add(NodeEntry{
		NodeID:        "node-a",
		MemberID:      1,
		LastHeartbeat: time.Now().Add(-time.Hour),
		DegradedAt:    time.Now().Add(-time.Hour),
		HealthState:   nodestate.HealthDegraded,
	})

	srv.checkNodeHealth(context.Background())

	_, ok := srv.nodeMap.Get("node-a")
	if ok {
		t.Fatal("expected node-a to be deregistered")
	}
}

func TestCheckNodeHealthKeepsDegradedBeforeTimeout(t *testing.T) {
	state := nodestate.New(slog.Default())
	_ = state.SetElector(nodestate.ElectorLeader)
	mgr := peerclient.NewManager(slog.Default(), "test-node", nil, state)
	srv := NewServer(
		slog.Default(),
		"test-cluster",
		storage.NewMemoryStore(),
		state,
		50*time.Millisecond,
		time.Hour, // deregTimeout — far in the future
		2,
		"test-node", 0, nil, 0, 0, mgr, nil, nil,
	)
	srv.nodeMap.SetReady()

	srv.nodeMap.Add(NodeEntry{
		NodeID:        "node-a",
		MemberID:      1,
		LastHeartbeat: time.Now().Add(-200 * time.Millisecond),
		DegradedAt:    time.Now(), // just became degraded
		HealthState:   nodestate.HealthDegraded,
	})

	srv.checkNodeHealth(context.Background())

	entry, ok := srv.nodeMap.Get("node-a")
	if !ok {
		t.Fatal("expected node-a to still exist")
	}
	if entry.HealthState != nodestate.HealthDegraded {
		t.Fatalf("expected degraded, got %s", entry.HealthState)
	}
}

func TestCheckNodeHealthClearsPrimaryOnDegradation(t *testing.T) {
	srv := newTestServer(50*time.Millisecond, 2)
	srv.nodeMap.SetReady()

	// Set node-a as the current Primary.
	srv.state.SetClusterPrimary(nodestate.NodeInfo{
		NodeID:            "node-a",
		MemberID:          1,
		PeerAdvertiseAddr: "10.0.0.1:2381",
	})

	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-a",
		MemberID:             1,
		PeerAdvertiseAddress: "10.0.0.1:2381",
		LastHeartbeat:        time.Now().Add(-200 * time.Millisecond),
		HealthState:          nodestate.HealthHealthy,
	})

	srv.checkNodeHealth(context.Background())

	// Primary should be cleared from ClusterState.
	cs := srv.state.ClusterState()
	if cs.Primary.NodeID != "" {
		t.Fatalf("expected primary cleared, got %s", cs.Primary.NodeID)
	}

	// Previous primary should be saved for election drain check.
	if prev := srv.loadPreviousPrimary(); prev.NodeID != "node-a" {
		t.Fatalf("expected previousPrimary=node-a, got %s", prev.NodeID)
	}
}

func TestCheckNodeHealthClearsPrimaryOnDeregistration(t *testing.T) {
	state := nodestate.New(slog.Default())
	_ = state.SetElector(nodestate.ElectorLeader)
	mgr := peerclient.NewManager(slog.Default(), "test-node", nil, state)
	srv := NewServer(
		slog.Default(),
		"test-cluster",
		storage.NewMemoryStore(),
		state,
		50*time.Millisecond,
		100*time.Millisecond, // deregTimeout
		2,
		"test-node", 0, nil, 0, 0, mgr, nil, nil,
	)
	srv.nodeMap.SetReady()

	// Set node-a as the current Primary.
	state.SetClusterPrimary(nodestate.NodeInfo{
		NodeID:            "node-a",
		MemberID:          1,
		PeerAdvertiseAddr: "10.0.0.1:2381",
	})

	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-a",
		MemberID:             1,
		PeerAdvertiseAddress: "10.0.0.1:2381",
		LastHeartbeat:        time.Now().Add(-time.Hour),
		DegradedAt:           time.Now().Add(-time.Hour),
		HealthState:          nodestate.HealthDegraded,
	})

	srv.checkNodeHealth(context.Background())

	// Node should be deregistered.
	if _, ok := srv.nodeMap.Get("node-a"); ok {
		t.Fatal("expected node-a to be deregistered")
	}

	// Primary should be cleared from ClusterState.
	cs := state.ClusterState()
	if cs.Primary.NodeID != "" {
		t.Fatalf("expected primary cleared, got %s", cs.Primary.NodeID)
	}

	// Previous primary should be saved.
	if prev := srv.loadPreviousPrimary(); prev.NodeID != "node-a" {
		t.Fatalf("expected previousPrimary=node-a, got %s", prev.NodeID)
	}
}

func TestCheckNodeHealthDoesNotClearNonPrimary(t *testing.T) {
	srv := newTestServer(50*time.Millisecond, 2)
	srv.nodeMap.SetReady()

	// Set node-b as the Primary — node-a is just a Replica.
	srv.state.SetClusterPrimary(nodestate.NodeInfo{
		NodeID:            "node-b",
		MemberID:          2,
		PeerAdvertiseAddr: "10.0.0.2:2381",
	})

	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-a",
		MemberID:             1,
		PeerAdvertiseAddress: "10.0.0.1:2381",
		LastHeartbeat:        time.Now().Add(-200 * time.Millisecond),
		HealthState:          nodestate.HealthHealthy,
	})
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-b",
		MemberID:             2,
		PeerAdvertiseAddress: "10.0.0.2:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthHealthy,
	})

	srv.checkNodeHealth(context.Background())

	// node-a degraded, but Primary (node-b) should remain.
	cs := srv.state.ClusterState()
	if cs.Primary.NodeID != "node-b" {
		t.Fatalf("expected primary=node-b, got %s", cs.Primary.NodeID)
	}
	if prev := srv.loadPreviousPrimary(); prev.NodeID != "" {
		t.Fatalf("expected no previousPrimary, got %s", prev.NodeID)
	}
}
