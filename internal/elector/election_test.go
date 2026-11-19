// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/storage"
)

func TestOnAcquireLeadershipReturnsBootstrapFailure(t *testing.T) {
	state := nodestate.New(slog.Default())
	store := storage.NewFailingStore(storage.NewMemoryStore())
	store.SetFailGet(true)

	srv := NewServer(
		slog.Default(),
		"test-cluster",
		store,
		state,
		50*time.Millisecond,
		100*time.Millisecond,
		2,
		"node-a",
		0,
		nil,
		0,
		0,
		nil,
		nil,
		nil,
	)
	r := &Runner{
		logger:   slog.Default(),
		nodeID:   "node-a",
		peerAddr: "10.0.0.1:2381",
		state:    state,
		server:   srv,
	}

	err := r.onAcquireLeadership(context.Background())
	if err == nil {
		t.Fatal("expected bootstrap failure")
	}
	if state.Elector() != nodestate.ElectorFollower {
		t.Fatalf("expected elector follower after failed acquire, got %s", state.Elector())
	}
	if srv.nodeMap.Ready() {
		t.Fatal("expected node map to remain unready after bootstrap failure")
	}
	if r.leaderCancel != nil {
		t.Fatal("expected leader cancel cleared after bootstrap failure")
	}
}
