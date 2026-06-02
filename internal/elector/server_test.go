// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/discovery"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
)

func TestSendHeartbeatUpdatesNodeState(t *testing.T) {
	state := nodestate.New(slog.Default())
	_ = state.SetElector(nodestate.ElectorLeader)

	srv := NewServer(
		slog.Default(),
		"test-cluster",
		storage.NewMemoryStore(),
		state,
		time.Second,
		0,
		2,
		"", 0, nil, 0, 0, nil, nil, nil,
	)
	srv.nodeMap.SetReady()
	srv.nodeMap.Add(NodeEntry{
		NodeID:        "node-a",
		MemberID:      1,
		LastHeartbeat: time.Now().Add(-time.Hour),
		HealthState:   nodestate.HealthLoading,
	})

	_, err := srv.SendHeartbeat(context.Background(), &proto.NodeState{
		NodeId:         "node-a",
		HealthState:    proto.HealthState_HEALTH_HEALTHY,
		PrimaryState:   proto.PrimaryState_PRIMARY_ACTIVE,
		LatestRevision: 42,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entry, ok := srv.nodeMap.Get("node-a")
	if !ok {
		t.Fatal("expected node-a in map")
	}
	if entry.HealthState != nodestate.HealthHealthy {
		t.Fatalf("expected healthy, got %s", entry.HealthState)
	}
	if entry.PrimaryState != nodestate.PrimaryActive {
		t.Fatalf("expected active, got %s", entry.PrimaryState)
	}
	if entry.LatestRevision != 42 {
		t.Fatalf("expected revision 42, got %d", entry.LatestRevision)
	}
}

func TestSendHeartbeatRejectsNonLeader(t *testing.T) {
	state := nodestate.New(slog.Default())
	// state is Follower by default

	srv := NewServer(
		slog.Default(),
		"test-cluster",
		storage.NewMemoryStore(),
		state,
		time.Second,
		0,
		2,
		"", 0, nil, 0, 0, nil, nil, nil,
	)

	_, err := srv.SendHeartbeat(context.Background(), &proto.NodeState{
		NodeId: "node-a",
	})
	if err == nil {
		t.Fatal("expected error when not elector leader")
	}
}

func TestSendHeartbeatRejectsUnknownNode(t *testing.T) {
	state := nodestate.New(slog.Default())
	_ = state.SetElector(nodestate.ElectorLeader)

	srv := NewServer(
		slog.Default(),
		"test-cluster",
		storage.NewMemoryStore(),
		state,
		time.Second,
		0,
		2,
		"", 0, nil, 0, 0, nil, nil, nil,
	)

	_, err := srv.SendHeartbeat(context.Background(), &proto.NodeState{
		NodeId: "node-unknown",
	})
	if err == nil {
		t.Fatal("expected error for unknown node")
	}
}

// TestStaleRegistrationReuseMemberID verifies that re-registering the same
// node_id after a simulated crash (without deregistration) reuses the
// previously allocated member_id.
func TestStaleRegistrationReuseMemberID(t *testing.T) {
	store := storage.NewMemoryStore()
	state := nodestate.New(slog.Default())
	_ = state.SetElector(nodestate.ElectorLeader)

	srv := NewServer(
		slog.Default(),
		"test-cluster",
		store,
		state,
		time.Second,
		0,
		2,
		"", 0, nil, 0, 0, nil, nil, nil,
	)
	srv.nodeMap.SetReady()

	// Seed an empty members.json so allocateOrReuseMemberID can read it.
	mf := discovery.MembersFile{
		ClusterID: "test-cluster",
		Members:   make(map[string]uint64),
	}
	if err := discovery.WriteMembersFile(context.Background(), store, mf); err != nil {
		t.Fatalf("seed members.json: %v", err)
	}

	// First registration.
	resp1, err := srv.RegisterNode(context.Background(), &proto.RegisterNodeRequest{
		NodeId:                 "node-a",
		ClientAdvertiseAddress: "https://node-a:2379",
		PeerAdvertiseAddress:   "https://node-a:2380",
	})
	if err != nil {
		t.Fatalf("first RegisterNode error = %v", err)
	}
	firstMemberID := resp1.GetMemberId()
	if firstMemberID == 0 {
		t.Fatal("first registration returned member_id 0")
	}

	// Simulate crash: do NOT call DeregisterNode.

	// Second registration with same node_id.
	resp2, err := srv.RegisterNode(context.Background(), &proto.RegisterNodeRequest{
		NodeId:                 "node-a",
		ClientAdvertiseAddress: "https://node-a:2379",
		PeerAdvertiseAddress:   "https://node-a:2380",
	})
	if err != nil {
		t.Fatalf("second RegisterNode error = %v", err)
	}

	if resp2.GetMemberId() != firstMemberID {
		t.Fatalf("member_id changed: first = %d, second = %d", firstMemberID, resp2.GetMemberId())
	}

	if srv.nodeMap.Count() != 1 {
		t.Fatalf("node map count = %d, want 1", srv.nodeMap.Count())
	}
}
