// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package peerclient

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/nodestate"
)

func TestApplyClusterStateSerializesPrimaryChangeCallbacks(t *testing.T) {
	t.Parallel()

	state := nodestate.New(slog.Default())
	manager := NewManager(slog.Default(), "node-a", nil, state)

	callbackEntered := make(chan struct{}, 2)
	releaseCallback := make(chan struct{})
	var inCallback atomic.Bool
	var overlaps atomic.Int32
	manager.SetPrimaryChangeFunc(func(bool) {
		if !inCallback.CompareAndSwap(false, true) {
			overlaps.Add(1)
		}
		callbackEntered <- struct{}{}
		<-releaseCallback
		inCallback.Store(false)
	})

	selfPrimary := nodestate.ClusterState{
		Primary: nodestate.NodeInfo{NodeID: "node-a"},
	}
	remotePrimary := nodestate.ClusterState{
		Primary: nodestate.NodeInfo{NodeID: "node-b"},
	}

	firstDone := make(chan struct{})
	go func() {
		manager.ApplyClusterState(context.Background(), selfPrimary)
		close(firstDone)
	}()

	select {
	case <-callbackEntered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first primary-change callback")
	}

	secondDone := make(chan struct{})
	go func() {
		manager.ApplyClusterState(context.Background(), remotePrimary)
		close(secondDone)
	}()

	select {
	case <-callbackEntered:
		t.Fatal("second primary-change callback overlapped the first")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseCallback)

	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for first ApplyClusterState")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for second ApplyClusterState")
	}
	if overlaps.Load() != 0 {
		t.Fatal("primary-change callbacks overlapped")
	}
}
