// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/peerclient"
	"github.com/netsy-dev/netsy/internal/storage"
)

type mockDB struct {
	revision int64
}

func (m *mockDB) LatestRevision() (int64, error) {
	return m.revision, nil
}

func newElectionTestServer() *Server {
	state := nodestate.New(slog.Default())
	_ = state.SetElector(nodestate.ElectorLeader)
	_ = state.SetHealth(nodestate.HealthHealthy)

	db := &mockDB{revision: 10}
	mgr := peerclient.NewManager(slog.Default(), "node-a", nil, state)

	return NewServer(
		slog.Default(),
		"test-cluster",
		storage.NewMemoryStore(),
		state,
		time.Second,
		0,
		2,
		"node-a",
		1000,
		db,
		0, // disabled quorum for simpler tests
		time.Second,
		mgr,
		nil,
		nil,
	)
}

func TestNeedsPrimaryElectionNoPrimary(t *testing.T) {
	srv := newElectionTestServer()
	if !srv.needsPrimaryElection() {
		t.Fatal("expected election needed when no primary set")
	}
}

func TestNeedsPrimaryElectionWithPrimary(t *testing.T) {
	srv := newElectionTestServer()
	srv.state.SetClusterPrimary(nodestate.NodeInfo{NodeID: "node-b"})
	if srv.needsPrimaryElection() {
		t.Fatal("expected no election needed when primary set")
	}
}

func TestElectPrimaryOnceDisabledQuorumSingleNode(t *testing.T) {
	srv := newElectionTestServer()
	srv.nodeMap.SetReady()
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-a",
		MemberID:             1,
		PeerAdvertiseAddress: "10.0.0.1:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthHealthy,
		PrimaryState:         nodestate.PrimaryReplica,
		LatestRevision:       10,
		StartTime:            1000,
	})

	elected, err := srv.electPrimaryOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elected.NodeID != "node-a" {
		t.Fatalf("expected node-a elected, got %s", elected.NodeID)
	}
	if elected.MemberID != 1 {
		t.Fatalf("expected member_id 1, got %d", elected.MemberID)
	}
}

func TestElectPrimaryOncePicksHighestRevision(t *testing.T) {
	srv := newElectionTestServer()
	srv.nodeMap.SetReady()
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-a",
		MemberID:             1,
		PeerAdvertiseAddress: "10.0.0.1:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthHealthy,
		PrimaryState:         nodestate.PrimaryReplica,
		LatestRevision:       10,
		StartTime:            1000,
	})
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-b",
		MemberID:             2,
		PeerAdvertiseAddress: "10.0.0.2:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthHealthy,
		PrimaryState:         nodestate.PrimaryReplica,
		LatestRevision:       20,
		StartTime:            900,
	})

	elected, err := srv.electPrimaryOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elected.NodeID != "node-b" {
		t.Fatalf("expected node-b (higher revision), got %s", elected.NodeID)
	}
}

func TestElectPrimaryOnceTieBreakByStartTime(t *testing.T) {
	srv := newElectionTestServer()
	srv.nodeMap.SetReady()
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-a",
		MemberID:             1,
		PeerAdvertiseAddress: "10.0.0.1:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthHealthy,
		PrimaryState:         nodestate.PrimaryReplica,
		LatestRevision:       10,
		StartTime:            900,
	})
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-b",
		MemberID:             2,
		PeerAdvertiseAddress: "10.0.0.2:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthHealthy,
		PrimaryState:         nodestate.PrimaryReplica,
		LatestRevision:       10,
		StartTime:            1000,
	})

	elected, err := srv.electPrimaryOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elected.NodeID != "node-b" {
		t.Fatalf("expected node-b (later start time), got %s", elected.NodeID)
	}
}

func TestElectPrimaryOncePreservesActivePrimary(t *testing.T) {
	srv := newElectionTestServer()
	srv.nodeMap.SetReady()
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-a",
		MemberID:             1,
		PeerAdvertiseAddress: "10.0.0.1:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthHealthy,
		PrimaryState:         nodestate.PrimaryReplica,
		LatestRevision:       20,
		StartTime:            1000,
	})
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-b",
		MemberID:             2,
		PeerAdvertiseAddress: "10.0.0.2:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthHealthy,
		PrimaryState:         nodestate.PrimaryActive,
		LatestRevision:       10,
		StartTime:            900,
	})

	elected, err := srv.electPrimaryOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elected.NodeID != "node-b" {
		t.Fatalf("expected node-b (already active), got %s", elected.NodeID)
	}
}

func TestElectPrimaryOnceFailsOnNonReplicaNonDegraded(t *testing.T) {
	srv := newElectionTestServer()
	srv.nodeMap.SetReady()
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-a",
		MemberID:             1,
		PeerAdvertiseAddress: "10.0.0.1:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthHealthy,
		PrimaryState:         nodestate.PrimaryReplica,
		LatestRevision:       10,
		StartTime:            1000,
	})
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-b",
		MemberID:             2,
		PeerAdvertiseAddress: "10.0.0.2:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthHealthy,
		PrimaryState:         nodestate.PrimaryStarting,
		LatestRevision:       10,
		StartTime:            900,
	})

	_, err := srv.electPrimaryOnce(context.Background())
	if err == nil {
		t.Fatal("expected error when non-degraded node has non-replica primary state")
	}
}

func TestElectPrimaryOnceIgnoresDegradedForPrimaryStateCheck(t *testing.T) {
	srv := newElectionTestServer()
	srv.nodeMap.SetReady()
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-a",
		MemberID:             1,
		PeerAdvertiseAddress: "10.0.0.1:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthHealthy,
		PrimaryState:         nodestate.PrimaryReplica,
		LatestRevision:       10,
		StartTime:            1000,
	})
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-b",
		MemberID:             2,
		PeerAdvertiseAddress: "10.0.0.2:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthDegraded,
		PrimaryState:         nodestate.PrimaryStarting, // degraded, so ignored
		LatestRevision:       10,
		StartTime:            900,
	})

	elected, err := srv.electPrimaryOnce(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Only node-a is healthy, so it should be elected.
	if elected.NodeID != "node-a" {
		t.Fatalf("expected node-a elected, got %s", elected.NodeID)
	}
}

func TestElectPrimaryOnceNoHealthyNodes(t *testing.T) {
	srv := newElectionTestServer()
	srv.nodeMap.SetReady()
	srv.nodeMap.Add(NodeEntry{
		NodeID:               "node-a",
		MemberID:             1,
		PeerAdvertiseAddress: "10.0.0.1:2381",
		LastHeartbeat:        time.Now(),
		HealthState:          nodestate.HealthDegraded,
		PrimaryState:         nodestate.PrimaryReplica,
		LatestRevision:       10,
		StartTime:            1000,
	})

	_, err := srv.electPrimaryOnce(context.Background())
	if err == nil {
		t.Fatal("expected error when no healthy nodes")
	}
}

func TestElectPrimaryOnceNoRegisteredNodes(t *testing.T) {
	srv := newElectionTestServer()
	srv.nodeMap.SetReady()

	_, err := srv.electPrimaryOnce(context.Background())
	if err == nil {
		t.Fatal("expected error when no registered nodes")
	}
}

func TestCheckPreviousPrimaryLocalReplica(t *testing.T) {
	srv := newElectionTestServer()
	srv.storePreviousPrimary(nodestate.NodeInfo{
		NodeID:            "node-a",
		PeerAdvertiseAddr: "10.0.0.1:2381",
	})
	// Default Primary State is Replica.
	err := srv.checkPreviousPrimary(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if prev := srv.loadPreviousPrimary(); prev.NodeID != "" {
		t.Fatalf("expected previousPrimary cleared, got %s", prev.NodeID)
	}
}

func TestCheckPreviousPrimaryLocalActive(t *testing.T) {
	srv := newElectionTestServer()
	_ = srv.state.SetPrimary(nodestate.PrimaryStarting)
	_ = srv.state.SetPrimary(nodestate.PrimaryActive)
	srv.storePreviousPrimary(nodestate.NodeInfo{
		NodeID:            "node-a",
		PeerAdvertiseAddr: "10.0.0.1:2381",
	})

	err := srv.checkPreviousPrimary(context.Background())
	if err == nil {
		t.Fatal("expected error when local node is still active primary")
	}
	// previousPrimary should NOT be cleared.
	if prev := srv.loadPreviousPrimary(); prev.NodeID != "node-a" {
		t.Fatalf("expected previousPrimary preserved, got %s", prev.NodeID)
	}
}

func TestCheckPreviousPrimaryLocalDraining(t *testing.T) {
	srv := newElectionTestServer()
	_ = srv.state.SetPrimary(nodestate.PrimaryStarting)
	_ = srv.state.SetPrimary(nodestate.PrimaryActive)
	_ = srv.state.SetPrimary(nodestate.PrimaryDraining)
	srv.storePreviousPrimary(nodestate.NodeInfo{
		NodeID:            "node-a",
		PeerAdvertiseAddr: "10.0.0.1:2381",
	})

	err := srv.checkPreviousPrimary(context.Background())
	if err == nil {
		t.Fatal("expected error when local node is still draining")
	}
	if prev := srv.loadPreviousPrimary(); prev.NodeID != "node-a" {
		t.Fatalf("expected previousPrimary preserved, got %s", prev.NodeID)
	}
}

func TestCheckPreviousPrimarySkipsWhenEmpty(t *testing.T) {
	srv := newElectionTestServer()
	err := srv.checkPreviousPrimary(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPreviousPrimaryConcurrentAccess(t *testing.T) {
	srv := newElectionTestServer()

	// previousPrimary is written by health checks and leadership callbacks,
	// and read by the election loop. This test is intentionally a
	// race-detector regression for the field-level synchronization contract.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				srv.storePreviousPrimary(nodestate.NodeInfo{
					NodeID:            "node-a",
					MemberID:          uint64(id),
					PeerAdvertiseAddr: "10.0.0.1:2381",
				})
				_ = srv.loadPreviousPrimary()
			}
		}(i)
	}
	wg.Wait()
}
