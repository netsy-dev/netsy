// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"testing"

	"github.com/netsy-dev/netsy/internal/storage"
)

func TestListSnapshots(t *testing.T) {
	store := &mockStorage{
		objects: []storage.ObjectInfo{
			{Key: "snapshots/0000000000000000100.netsy", Size: 1000},
			{Key: "snapshots/0000000000000000300.netsy", Size: 3000},
			{Key: "snapshots/0000000000000000200.netsy", Size: 2000},
		},
	}

	snapshots, err := ListSnapshots(context.Background(), store)
	if err != nil {
		t.Fatalf("ListSnapshots: unexpected error: %v", err)
	}

	if len(snapshots) != 3 {
		t.Fatalf("ListSnapshots: got %d snapshots, want 3", len(snapshots))
	}

	// Should be sorted newest first
	if snapshots[0].Revision != 300 {
		t.Errorf("snapshots[0].Revision = %d, want 300", snapshots[0].Revision)
	}
	if snapshots[1].Revision != 200 {
		t.Errorf("snapshots[1].Revision = %d, want 200", snapshots[1].Revision)
	}
	if snapshots[2].Revision != 100 {
		t.Errorf("snapshots[2].Revision = %d, want 100", snapshots[2].Revision)
	}
}

func TestGetLatestSnapshot_Found(t *testing.T) {
	store := &mockStorage{
		objects: []storage.ObjectInfo{
			{Key: "snapshots/0000000000000000100.netsy", Size: 1000},
			{Key: "snapshots/0000000000000000300.netsy", Size: 3000},
			{Key: "snapshots/0000000000000000200.netsy", Size: 2000},
		},
	}

	info, err := GetLatestSnapshot(context.Background(), store)
	if err != nil {
		t.Fatalf("GetLatestSnapshot: unexpected error: %v", err)
	}

	if !info.Found {
		t.Fatal("GetLatestSnapshot: expected Found=true")
	}
	if info.Revision != 300 {
		t.Errorf("GetLatestSnapshot: Revision = %d, want 300", info.Revision)
	}
	if info.Size != 3000 {
		t.Errorf("GetLatestSnapshot: Size = %d, want 3000", info.Size)
	}
}

func TestGetLatestSnapshot_NotFound(t *testing.T) {
	store := &mockStorage{objects: []storage.ObjectInfo{}}

	info, err := GetLatestSnapshot(context.Background(), store)
	if err != nil {
		t.Fatalf("GetLatestSnapshot: unexpected error: %v", err)
	}

	if info.Found {
		t.Fatal("GetLatestSnapshot: expected Found=false")
	}
}

func TestListSnapshots_SkipsInvalidFiles(t *testing.T) {
	store := &mockStorage{
		objects: []storage.ObjectInfo{
			{Key: "snapshots/0000000000000000100.netsy", Size: 1000},
			{Key: "snapshots/notanumber.netsy", Size: 500},
			{Key: "snapshots/readme.txt", Size: 100},
		},
	}

	snapshots, err := ListSnapshots(context.Background(), store)
	if err != nil {
		t.Fatalf("ListSnapshots: unexpected error: %v", err)
	}

	if len(snapshots) != 1 {
		t.Fatalf("ListSnapshots: got %d snapshots, want 1", len(snapshots))
	}
	if snapshots[0].Revision != 100 {
		t.Errorf("snapshots[0].Revision = %d, want 100", snapshots[0].Revision)
	}
}
