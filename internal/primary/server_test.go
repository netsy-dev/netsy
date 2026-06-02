// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
)

func newTestServer(t *testing.T, state *nodestate.State, heartbeatInterval time.Duration, degradationCount int) *Server {
	t.Helper()
	return &Server{
		logger:            slog.Default(),
		state:             state,
		replicas:          NewReplicas(),
		followStreams:     make(map[string]*followSession),
		leaderTxnGate:     make(chan struct{}, 1),
		heartbeatInterval: heartbeatInterval,
		degradationCount:  degradationCount,
	}
}

func TestSendHeartbeatRequiresPrimary(t *testing.T) {
	state := nodestate.New(slog.Default())
	srv := newTestServer(t, state, 100*time.Millisecond, 2)

	// State is PrimaryReplica by default, so SendHeartbeat should fail
	_, err := srv.SendHeartbeat(context.Background(), &proto.NodeState{
		NodeId:      "node-a",
		HealthState: proto.HealthState_HEALTH_HEALTHY,
	})
	if err == nil {
		t.Fatal("expected error when node is not primary")
	}
}

func TestSendHeartbeatSuccess(t *testing.T) {
	state := nodestate.New(slog.Default())
	srv := newTestServer(t, state, 100*time.Millisecond, 2)

	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatal(err)
	}

	srv.replicas.Add("node-a")

	_, err := srv.SendHeartbeat(context.Background(), &proto.NodeState{
		NodeId:         "node-a",
		HealthState:    proto.HealthState_HEALTH_HEALTHY,
		PrimaryState:   proto.PrimaryState_PRIMARY_REPLICA,
		LatestRevision: 10,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entry, ok := srv.replicas.Get("node-a")
	if !ok {
		t.Fatal("expected node-a in replica map")
	}
	if entry.Health() != nodestate.HealthHealthy {
		t.Fatalf("expected healthy, got %s", entry.Health())
	}
	if entry.LatestRevision.Load() != 10 {
		t.Fatalf("expected revision 10, got %d", entry.LatestRevision.Load())
	}
}

func TestSendHeartbeatNotRegistered(t *testing.T) {
	state := nodestate.New(slog.Default())
	srv := newTestServer(t, state, 100*time.Millisecond, 2)

	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatal(err)
	}

	_, err := srv.SendHeartbeat(context.Background(), &proto.NodeState{
		NodeId:      "node-unknown",
		HealthState: proto.HealthState_HEALTH_HEALTHY,
	})
	if err == nil {
		t.Fatal("expected error for unknown replica")
	}
}

func TestDegradationCheck(t *testing.T) {
	state := nodestate.New(slog.Default())
	srv := newTestServer(t, state, 50*time.Millisecond, 2)

	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatal(err)
	}

	entry := srv.replicas.Add("node-a")
	entry.SetHealth(nodestate.HealthHealthy)
	entry.LastHeartbeat.Store(time.Now().Add(-200 * time.Millisecond).UnixNano())

	srv.checkDegradation()

	if entry.Health() != nodestate.HealthDegraded {
		t.Fatal("expected node-a to be marked degraded")
	}
}

func TestDegradationSkipsAlreadyDegraded(t *testing.T) {
	state := nodestate.New(slog.Default())
	srv := newTestServer(t, state, 50*time.Millisecond, 2)

	entry := srv.replicas.Add("node-a")
	entry.SetHealth(nodestate.HealthDegraded)
	entry.LastHeartbeat.Store(time.Now().Add(-time.Hour).UnixNano())

	srv.checkDegradation()

	if entry.Health() != nodestate.HealthDegraded {
		t.Fatal("expected node-a to remain degraded")
	}
}

func TestGracefulDrainActivePrimary(t *testing.T) {
	store := storage.NewMemoryStore()
	state := nodestate.New(slog.Default())
	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatal(err)
	}
	if err := state.SetPrimary(nodestate.PrimaryActive); err != nil {
		t.Fatal(err)
	}

	srv := &Server{
		logger: slog.Default(),
		state:  state,
		chunkBuffer: newChunkBuffer(
			slog.Default(), state, store, "node-a", 0, 0, nil,
		),
		replicas:      NewReplicas(),
		followStreams: make(map[string]*followSession),
	}

	// Buffer a record so the flush has work to do.
	record := &proto.Record{Revision: 1, Key: []byte("key"), Value: []byte("val")}
	srv.chunkBuffer.records = append(srv.chunkBuffer.records, record)
	srv.chunkBuffer.bytes = 10

	ctx := context.Background()
	if err := srv.GracefulDrain(ctx); err != nil {
		t.Fatalf("GracefulDrain error: %v", err)
	}

	if state.Primary() != nodestate.PrimaryDraining {
		t.Fatalf("expected draining, got %s", state.Primary())
	}

	// Verify chunk buffer was flushed.
	if len(srv.chunkBuffer.records) != 0 {
		t.Fatalf("expected chunk buffer empty, got %d records", len(srv.chunkBuffer.records))
	}
}

// TestGracefulShutdownFlushesBeforeDeregister verifies that GracefulDrain
// flushes all buffered records to object storage before proceeding with
// the deregistration path. This ensures no data is lost during graceful
// shutdown of a Primary node.
func TestGracefulShutdownFlushesBeforeDeregister(t *testing.T) {
	store := storage.NewMemoryStore()
	state := nodestate.New(slog.Default())
	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatal(err)
	}
	if err := state.SetPrimary(nodestate.PrimaryActive); err != nil {
		t.Fatal(err)
	}

	srv := &Server{
		logger: slog.Default(),
		state:  state,
		chunkBuffer: newChunkBuffer(
			slog.Default(), state, store, "node-a", 0, 0, nil,
		),
		replicas:      NewReplicas(),
		followStreams: make(map[string]*followSession),
	}

	// Buffer multiple records.
	records := []*proto.Record{
		{Revision: 1, Key: []byte("key-1"), Value: []byte("val-1"), LeaderId: "node-a"},
		{Revision: 2, Key: []byte("key-2"), Value: []byte("val-2"), LeaderId: "node-a"},
		{Revision: 3, Key: []byte("key-3"), Value: []byte("val-3"), LeaderId: "node-a"},
	}
	for _, r := range records {
		srv.chunkBuffer.records = append(srv.chunkBuffer.records, r)
		srv.chunkBuffer.bytes += 10
	}

	if err := srv.GracefulDrain(context.Background()); err != nil {
		t.Fatalf("GracefulDrain error: %v", err)
	}

	// Chunk buffer should be empty.
	if len(srv.chunkBuffer.records) != 0 {
		t.Fatalf("expected chunk buffer empty, got %d records", len(srv.chunkBuffer.records))
	}

	// Records should be flushed to object storage.
	objects, err := store.List(context.Background(), "chunks/")
	if err != nil {
		t.Fatalf("store.List() error = %v", err)
	}
	if len(objects) == 0 {
		t.Fatal("expected chunk file(s) in object storage after flush")
	}

	// State should be Draining.
	if state.Primary() != nodestate.PrimaryDraining {
		t.Fatalf("expected draining, got %s", state.Primary())
	}
}

func TestGracefulDrainNoOpForReplica(t *testing.T) {
	state := nodestate.New(slog.Default())
	srv := &Server{
		logger: slog.Default(),
		state:  state,
	}

	if err := srv.GracefulDrain(context.Background()); err != nil {
		t.Fatalf("GracefulDrain for replica should succeed: %v", err)
	}
}

func TestGracefulDrainFromStarting(t *testing.T) {
	store := storage.NewMemoryStore()
	state := nodestate.New(slog.Default())
	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatal(err)
	}

	srv := &Server{
		logger: slog.Default(),
		state:  state,
		chunkBuffer: newChunkBuffer(
			slog.Default(), state, store, "node-a", 0, 0, nil,
		),
	}

	if err := srv.GracefulDrain(context.Background()); err != nil {
		t.Fatalf("GracefulDrain error: %v", err)
	}

	if state.Primary() != nodestate.PrimaryDraining {
		t.Fatalf("expected draining, got %s", state.Primary())
	}
}

func TestResignLeadership(t *testing.T) {
	state := nodestate.New(slog.Default())
	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatal(err)
	}
	if err := state.SetPrimary(nodestate.PrimaryActive); err != nil {
		t.Fatal(err)
	}
	if err := state.SetPrimary(nodestate.PrimaryDraining); err != nil {
		t.Fatal(err)
	}

	srv := newTestServer(t, state, 0, 0)

	if err := srv.ResignLeadership(); err != nil {
		t.Fatalf("ResignLeadership error: %v", err)
	}

	if state.Primary() != nodestate.PrimaryReplica {
		t.Fatalf("expected replica, got %s", state.Primary())
	}
}

func TestResignLeadershipRequiresDraining(t *testing.T) {
	state := nodestate.New(slog.Default())
	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatal(err)
	}
	if err := state.SetPrimary(nodestate.PrimaryActive); err != nil {
		t.Fatal(err)
	}

	srv := newTestServer(t, state, 0, 0)

	if err := srv.ResignLeadership(); err == nil {
		t.Fatal("expected error when not draining")
	}
}
