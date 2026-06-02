// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/storage"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

func TestRequiredReceipts(t *testing.T) {
	tests := []struct {
		name      string
		quorum    int
		nodeCount int
		want      int
	}{
		{"disabled", 0, 5, 0},
		{"static_2", 2, 5, 2},
		{"static_1", 1, 3, 1},
		{"majority_7_nodes", -1, 7, 3},
		{"majority_5_nodes", -1, 5, 2},
		{"majority_4_nodes", -1, 4, 2},
		{"majority_3_nodes", -1, 3, 1},
		{"majority_2_nodes", -1, 2, 1},
		{"majority_1_node", -1, 1, 0},
		{"majority_0_nodes", -1, 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := requiredReceipts(tt.quorum, tt.nodeCount)
			if got != tt.want {
				t.Fatalf("requiredReceipts(%d, %d) = %d, want %d",
					tt.quorum, tt.nodeCount, got, tt.want)
			}
		})
	}
}

func TestSelectTxnPath(t *testing.T) {
	tests := []struct {
		name             string
		quorum           int
		nodeCount        int
		healthyForQuorum int
		wantQuorum       bool
		wantRequired     int
	}{
		{"disabled_quorum", 0, 3, 2, false, 0},
		{"majority_met", -1, 3, 1, true, 1},
		{"majority_not_met", -1, 3, 0, false, 0},
		{"static_met", 2, 5, 2, true, 2},
		{"static_not_met", 2, 5, 1, false, 0},
		{"single_node_majority", -1, 1, 0, false, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := selectTxnStrategy(tt.quorum, tt.nodeCount, tt.healthyForQuorum)
			if s.useQuorum != tt.wantQuorum {
				t.Fatalf("useQuorum = %v, want %v", s.useQuorum, tt.wantQuorum)
			}
			if s.useQuorum && s.requiredReceipts != tt.wantRequired {
				t.Fatalf("requiredReceipts = %d, want %d", s.requiredReceipts, tt.wantRequired)
			}
		})
	}
}

func TestReceiptCollectorCompletesOnThreshold(t *testing.T) {
	c := newReceiptCollector(42, 2)

	c.collectReceipt("node-a")
	if c.isComplete() {
		t.Fatal("should not be complete with 1 receipt")
	}

	c.collectReceipt("node-b")
	if !c.isComplete() {
		t.Fatal("should be complete with 2 receipts")
	}

	ok := c.wait(time.Millisecond)
	if !ok {
		t.Fatal("wait should return true when quorum met")
	}
}

func TestReceiptCollectorDeduplicatesReceipts(t *testing.T) {
	c := newReceiptCollector(42, 2)

	c.collectReceipt("node-a")
	c.collectReceipt("node-a")
	if c.isComplete() {
		t.Fatal("duplicate receipt should not count twice")
	}

	c.collectReceipt("node-b")
	if !c.isComplete() {
		t.Fatal("should be complete with 2 distinct receipts")
	}
}

func TestReceiptCollectorTimeout(t *testing.T) {
	c := newReceiptCollector(42, 2)

	c.collectReceipt("node-a")
	ok := c.wait(10 * time.Millisecond)
	if ok {
		t.Fatal("wait should return false on timeout")
	}
}

func TestReceiptCollectorUnackedQuorumNodeIDs(t *testing.T) {
	c := newReceiptCollector(42, 2)

	// node-a: acked
	// node-b: healthy + receipted = eligible, not acked → should appear
	// node-c: healthy + receipted = eligible, not acked → should appear
	// node-d: degraded + receipted = not eligible → should NOT appear
	// node-e: healthy + no prior receipt = not eligible → should NOT appear
	replicaA := &Replica{NodeID: "node-a"}
	replicaA.SetHealth(nodestate.HealthHealthy)
	replicaA.ReceiptCount.Store(1)

	replicaB := &Replica{NodeID: "node-b"}
	replicaB.SetHealth(nodestate.HealthHealthy)
	replicaB.ReceiptCount.Store(1)

	replicaC := &Replica{NodeID: "node-c"}
	replicaC.SetHealth(nodestate.HealthHealthy)
	replicaC.ReceiptCount.Store(3)

	replicaD := &Replica{NodeID: "node-d"}
	replicaD.SetHealth(nodestate.HealthDegraded)
	replicaD.ReceiptCount.Store(2)

	replicaE := &Replica{NodeID: "node-e"}
	replicaE.SetHealth(nodestate.HealthHealthy)

	replicas := []*Replica{replicaA, replicaB, replicaC, replicaD, replicaE}
	c.collectReceipt("node-a")

	missing := c.unackedQuorumNodeIDs(replicas)
	if len(missing) != 2 {
		t.Fatalf("unackedQuorumNodeIDs = %v, want 2 entries", missing)
	}

	found := make(map[string]bool)
	for _, id := range missing {
		found[id] = true
	}
	if !found["node-b"] || !found["node-c"] {
		t.Fatalf("unackedQuorumNodeIDs = %v, want node-b and node-c", missing)
	}
}

func TestReceiptCollectorIgnoresReceiptAfterDone(t *testing.T) {
	c := newReceiptCollector(42, 1)

	c.collectReceipt("node-a")
	if !c.isComplete() {
		t.Fatal("should be complete")
	}

	// Should not panic or change state.
	c.collectReceipt("node-b")
	if !c.isComplete() {
		t.Fatal("should still be complete")
	}
}

// TestQuorumRollbackAndRetryViaPath1 verifies the quorum rollback path:
// when receipts are not received within the timeout, the transaction is
// rolled back, timed-out replicas are degraded, and the same revision is
// reused on retry via Path 1.
func TestQuorumRollbackAndRetryViaPath1(t *testing.T) {
	db := openPrimaryTestDB(t)
	store := storage.NewMemoryStore()
	state := nodestate.New(slog.Default())
	cfg := &config.Config{
		NodeConfig: config.NodeConfig{
			NodeID:  "node-a",
			DataDir: t.TempDir(),
		},
		ClusterConfig: config.ClusterConfig{
			Replication: config.ReplicationConfig{
				Quorum: intPtr(-1),
				ChunkBuffer: config.ChunkBufferConfig{
					ThresholdSizeMB:     0,
					ThresholdAgeMinutes: 0,
				},
			},
		},
	}

	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatalf("SetPrimary(Starting) error = %v", err)
	}
	if err := state.SetPrimary(nodestate.PrimaryActive); err != nil {
		t.Fatalf("SetPrimary(Active) error = %v", err)
	}

	// Set cluster to 3 nodes so majority quorum requires 1 receipt.
	state.SetClusterState(nodestate.ClusterState{NodeCount: 3})

	srv := &Server{
		logger:               slog.Default(),
		config:               cfg,
		db:                   db,
		storageClient:        store,
		state:                state,
		replicas:             NewReplicas(),
		followStreams:        make(map[string]*followSession),
		leaderTxnGate:        make(chan struct{}, 1),
		quorumReceiptTimeout: 10 * time.Millisecond,
	}
	srv.chunkBuffer = newChunkBuffer(slog.Default(), state, store, cfg.NodeID, 0, 0, nil)
	if err := srv.initializeRevisionCounter(); err != nil {
		t.Fatalf("initializeRevisionCounter() error = %v", err)
	}

	// Add a healthy replica with prior receipts so quorum path is selected.
	replica := srv.replicas.Add("replica-b")
	replica.SetHealth(nodestate.HealthHealthy)
	replica.ReceiptCount.Store(1)

	// Build a valid create TxnRequest.
	createReq := &pb.TxnRequest{
		Compare: []*pb.Compare{{
			Key:         []byte("test-key"),
			Target:      pb.Compare_MOD,
			Result:      pb.Compare_EQUAL,
			TargetUnion: &pb.Compare_ModRevision{ModRevision: 0},
		}},
		Success: []*pb.RequestOp{{
			Request: &pb.RequestOp_RequestPut{
				RequestPut: &pb.PutRequest{
					Key:   []byte("test-key"),
					Value: []byte("test-value"),
				},
			},
		}},
	}

	// Quorum write should fail (no receipt within 10ms).
	_, _, err := srv.LeaderTxn(context.Background(), createReq)
	if err == nil {
		t.Fatal("LeaderTxn() expected quorum rollback error")
	}
	if !strings.Contains(err.Error(), "quorum not met") {
		t.Fatalf("LeaderTxn() error = %v, want quorum error", err)
	}

	// Replica should be marked degraded.
	entry, ok := srv.replicas.Get("replica-b")
	if !ok {
		t.Fatal("expected replica-b in map")
	}
	if entry.Health() != nodestate.HealthDegraded {
		t.Fatalf("replica health = %s, want degraded", entry.Health())
	}

	// nextRevisionID should NOT have advanced.
	if got := srv.nextRevisionID.Load(); got != 1 {
		t.Fatalf("nextRevisionID = %d, want 1 (not incremented)", got)
	}

	// Give chunk buffer flush goroutine a moment to complete.
	time.Sleep(50 * time.Millisecond)

	// Retry: quorum path should fall back to Path 1 (replica is degraded).
	_, resp, err := srv.LeaderTxn(context.Background(), createReq)
	if err != nil {
		t.Fatalf("LeaderTxn() retry error = %v", err)
	}
	if !resp.Succeeded {
		t.Fatal("LeaderTxn() retry did not succeed")
	}

	// The SAME revision (1) should have been used.
	if resp.Header.Revision != 1 {
		t.Fatalf("retry revision = %d, want 1 (reused)", resp.Header.Revision)
	}

	// Now nextRevisionID should be 2.
	if got := srv.nextRevisionID.Load(); got != 2 {
		t.Fatalf("nextRevisionID after retry = %d, want 2", got)
	}
}

// TestMembershipChangeDropsToPath1 verifies that reducing the number of
// healthy replicas below the quorum requirement causes selectTxnStrategy
// to fall back to Path 1 (sync object storage writes).
func TestMembershipChangeDropsToPath1(t *testing.T) {
	// 3 nodes, majority quorum → requires 1 receipt.
	s := selectTxnStrategy(-1, 3, 1)
	if !s.useQuorum {
		t.Fatal("expected quorum path with 1 healthy replica in 3-node cluster")
	}

	// Simulate membership change: healthy replica count drops to 0.
	s = selectTxnStrategy(-1, 3, 0)
	if s.useQuorum {
		t.Fatal("expected sync path with 0 healthy replicas")
	}

	// Also verify with static quorum.
	s = selectTxnStrategy(2, 5, 2)
	if !s.useQuorum {
		t.Fatal("expected quorum path with 2 healthy replicas and static quorum 2")
	}

	s = selectTxnStrategy(2, 5, 1)
	if s.useQuorum {
		t.Fatal("expected sync path with 1 healthy replica and static quorum 2")
	}
}

// intPtr returns a pointer to the given int value.
func intPtr(v int) *int {
	return &v
}
