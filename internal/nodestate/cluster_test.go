// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package nodestate

import (
	"testing"
)

func TestClusterStateInitiallyEmpty(t *testing.T) {
	s := newTestState()
	cs := s.ClusterState()
	if cs.Elector.NodeID != "" {
		t.Fatalf("expected empty elector node_id, got %q", cs.Elector.NodeID)
	}
	if cs.Primary.NodeID != "" {
		t.Fatalf("expected empty primary node_id, got %q", cs.Primary.NodeID)
	}
}

func TestSetClusterState(t *testing.T) {
	s := newTestState()
	s.SetClusterState(ClusterState{
		Elector: NodeInfo{NodeID: "node-a", MemberID: 1, PeerAdvertiseAddr: "10.0.0.1:2381"},
		Primary: NodeInfo{NodeID: "node-b", MemberID: 2, PeerAdvertiseAddr: "10.0.0.2:2381"},
	})

	cs := s.ClusterState()
	if cs.Elector.NodeID != "node-a" {
		t.Fatalf("elector node_id = %q, want %q", cs.Elector.NodeID, "node-a")
	}
	if cs.Elector.MemberID != 1 {
		t.Fatalf("elector member_id = %d, want %d", cs.Elector.MemberID, 1)
	}
	if cs.Primary.NodeID != "node-b" {
		t.Fatalf("primary node_id = %q, want %q", cs.Primary.NodeID, "node-b")
	}
}

func TestSetClusterElector(t *testing.T) {
	s := newTestState()
	s.SetClusterState(ClusterState{
		Elector: NodeInfo{NodeID: "node-a", PeerAdvertiseAddr: "10.0.0.1:2381"},
		Primary: NodeInfo{NodeID: "node-b", PeerAdvertiseAddr: "10.0.0.2:2381"},
	})

	s.SetClusterElector(NodeInfo{NodeID: "node-c", PeerAdvertiseAddr: "10.0.0.3:2381"})

	cs := s.ClusterState()
	if cs.Elector.NodeID != "node-c" {
		t.Fatalf("elector node_id = %q, want %q", cs.Elector.NodeID, "node-c")
	}
	if cs.Primary.NodeID != "node-b" {
		t.Fatalf("primary should be unchanged, got %q", cs.Primary.NodeID)
	}
}

func TestSetClusterPrimary(t *testing.T) {
	s := newTestState()
	s.SetClusterState(ClusterState{
		Elector: NodeInfo{NodeID: "node-a", PeerAdvertiseAddr: "10.0.0.1:2381"},
		Primary: NodeInfo{NodeID: "node-b", PeerAdvertiseAddr: "10.0.0.2:2381"},
	})

	s.SetClusterPrimary(NodeInfo{NodeID: "node-c", MemberID: 3, PeerAdvertiseAddr: "10.0.0.3:2381"})

	cs := s.ClusterState()
	if cs.Primary.NodeID != "node-c" {
		t.Fatalf("primary node_id = %q, want %q", cs.Primary.NodeID, "node-c")
	}
	if cs.Primary.MemberID != 3 {
		t.Fatalf("primary member_id = %d, want %d", cs.Primary.MemberID, 3)
	}
	if cs.Elector.NodeID != "node-a" {
		t.Fatalf("elector should be unchanged, got %q", cs.Elector.NodeID)
	}
}
