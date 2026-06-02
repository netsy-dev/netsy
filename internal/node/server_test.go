// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/peerclient"
	"github.com/netsy-dev/netsy/internal/proto"
)

type mockRevisionSource struct {
	revision int64
	err      error
}

func (m *mockRevisionSource) LatestRevision() (int64, error) {
	return m.revision, m.err
}

type mockWatchRevisionSource struct {
	minRevision int64
	floorErr    error
}

func (m *mockWatchRevisionSource) MinWatchRevision() int64 {
	return m.minRevision
}

func (m *mockWatchRevisionSource) SetWatchAdmissionFloor(_ int64) error {
	return m.floorErr
}

func newTestServer(revision int64) *Server {
	state := nodestate.New(slog.Default())
	_ = state.SetHealth(nodestate.HealthHealthy)

	mgr := peerclient.NewManager(slog.Default(), "node-a", nil, state)

	return NewServer(
		slog.Default(),
		"node-a",
		1000,
		state,
		&mockRevisionSource{revision: revision},
		mgr,
		&mockWatchRevisionSource{minRevision: -1},
	)
}

func TestGetNodeState(t *testing.T) {
	srv := newTestServer(42)

	resp, err := srv.GetNodeState(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.GetNodeId() != "node-a" {
		t.Fatalf("expected node-a, got %s", resp.GetNodeId())
	}
	if resp.GetLatestRevision() != 42 {
		t.Fatalf("expected revision 42, got %d", resp.GetLatestRevision())
	}
	if resp.GetStartTime() != 1000 {
		t.Fatalf("expected start time 1000, got %d", resp.GetStartTime())
	}
	if resp.GetHealthState() != proto.HealthState_HEALTH_HEALTHY {
		t.Fatalf("expected healthy, got %v", resp.GetHealthState())
	}
	if resp.GetPrimaryState() != proto.PrimaryState_PRIMARY_REPLICA {
		t.Fatalf("expected replica, got %v", resp.GetPrimaryState())
	}
	if resp.GetElectorState() != proto.ElectorState_ELECTOR_FOLLOWER {
		t.Fatalf("expected follower, got %v", resp.GetElectorState())
	}
}

func TestGetMinWatchRevisionWithActiveWatches(t *testing.T) {
	state := nodestate.New(slog.Default())
	_ = state.SetHealth(nodestate.HealthHealthy)
	state.SetCommitted(100)

	mgr := peerclient.NewManager(slog.Default(), "node-a", nil, state)

	srv := NewServer(
		slog.Default(),
		"node-a",
		1000,
		state,
		&mockRevisionSource{revision: 100},
		mgr,
		&mockWatchRevisionSource{minRevision: 50},
	)

	resp, err := srv.GetMinWatchRevision(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetMinRevision() != 50 {
		t.Fatalf("expected min_revision 50, got %d", resp.GetMinRevision())
	}
}

func TestGetMinWatchRevisionNoWatches(t *testing.T) {
	state := nodestate.New(slog.Default())
	_ = state.SetHealth(nodestate.HealthHealthy)
	state.SetCommitted(100)

	mgr := peerclient.NewManager(slog.Default(), "node-a", nil, state)

	srv := NewServer(
		slog.Default(),
		"node-a",
		1000,
		state,
		&mockRevisionSource{revision: 100},
		mgr,
		&mockWatchRevisionSource{minRevision: -1},
	)

	resp, err := srv.GetMinWatchRevision(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetMinRevision() != 100 {
		t.Fatalf("expected min_revision 100 (committed_revision), got %d", resp.GetMinRevision())
	}
}

func TestSendCompactionNoticeAccepted(t *testing.T) {
	state := nodestate.New(slog.Default())
	_ = state.SetHealth(nodestate.HealthHealthy)

	mgr := peerclient.NewManager(slog.Default(), "node-a", nil, state)

	srv := NewServer(
		slog.Default(),
		"node-a",
		1000,
		state,
		&mockRevisionSource{revision: 100},
		mgr,
		&mockWatchRevisionSource{minRevision: 50, floorErr: nil},
	)

	resp, err := srv.SendCompactionNotice(context.Background(), &proto.CompactionNotice{
		CompactionRevision: 40,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.GetConfirmed() {
		t.Fatal("expected confirmed=true")
	}
}

func TestSendCompactionNoticeRejected(t *testing.T) {
	state := nodestate.New(slog.Default())
	_ = state.SetHealth(nodestate.HealthHealthy)

	mgr := peerclient.NewManager(slog.Default(), "node-a", nil, state)

	srv := NewServer(
		slog.Default(),
		"node-a",
		1000,
		state,
		&mockRevisionSource{revision: 100},
		mgr,
		&mockWatchRevisionSource{minRevision: 50, floorErr: fmt.Errorf("active watch exists below proposed compaction revision")},
	)

	resp, err := srv.SendCompactionNotice(context.Background(), &proto.CompactionNotice{
		CompactionRevision: 60,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetConfirmed() {
		t.Fatal("expected confirmed=false")
	}
}

func TestPushClusterStateMissingElector(t *testing.T) {
	srv := newTestServer(0)

	_, err := srv.PushClusterState(context.Background(), &proto.ClusterState{})
	if err == nil {
		t.Fatal("expected error for missing elector info")
	}
}

func TestPushClusterStateMissingElectorNodeID(t *testing.T) {
	srv := newTestServer(0)

	_, err := srv.PushClusterState(context.Background(), &proto.ClusterState{
		Elector: &proto.NodeInfo{},
	})
	if err == nil {
		t.Fatal("expected error for empty elector node_id")
	}
}

func TestPushClusterStateUpdatesState(t *testing.T) {
	srv := newTestServer(0)

	cs := &proto.ClusterState{
		Elector: &proto.NodeInfo{
			NodeId:               "elector-1",
			MemberId:             1,
			PeerAdvertiseAddress: "10.0.0.1:2381",
		},
		Primary: &proto.NodeInfo{
			NodeId:               "primary-1",
			MemberId:             2,
			PeerAdvertiseAddress: "10.0.0.2:2381",
		},
	}

	_, err := srv.PushClusterState(context.Background(), cs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := srv.state.ClusterState()
	if got.Elector.NodeID != "elector-1" {
		t.Fatalf("expected elector-1, got %s", got.Elector.NodeID)
	}
	if got.Primary.NodeID != "primary-1" {
		t.Fatalf("expected primary-1, got %s", got.Primary.NodeID)
	}
	if got.Primary.MemberID != 2 {
		t.Fatalf("expected member_id 2, got %d", got.Primary.MemberID)
	}
}
