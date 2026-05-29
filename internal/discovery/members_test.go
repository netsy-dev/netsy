// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"context"
	"errors"
	"testing"

	"github.com/netsy-dev/netsy/internal/storage"
)

func TestReadMembersFile_NotFound(t *testing.T) {
	store := storage.NewMemoryStore()

	_, err := ReadMembersFile(context.Background(), store)
	if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestWriteMembersFile_CreateNew(t *testing.T) {
	store := storage.NewMemoryStore()
	mf := MembersFile{
		ClusterID: "cluster-1",
		Members:   map[string]uint64{"node-1": 12345},
	}

	if err := WriteMembersFile(context.Background(), store, mf); err != nil {
		t.Fatalf("WriteMembersFile() error = %v", err)
	}

	// Verify the file was written.
	got, err := ReadMembersFile(context.Background(), store)
	if err != nil {
		t.Fatalf("ReadMembersFile() error = %v", err)
	}
	if got.ClusterID != "cluster-1" {
		t.Errorf("ClusterID = %q, want %q", got.ClusterID, "cluster-1")
	}
	if got.Members["node-1"] != 12345 {
		t.Errorf("Members[node-1] = %d, want %d", got.Members["node-1"], 12345)
	}
	if got.ETag == "" {
		t.Error("expected non-empty ETag after read")
	}
}

func TestWriteMembersFile_PreconditionFail(t *testing.T) {
	store := storage.NewMemoryStore()
	mf := MembersFile{
		ClusterID: "cluster-1",
		Members:   map[string]uint64{"node-1": 12345},
	}

	// Create the file first.
	if err := WriteMembersFile(context.Background(), store, mf); err != nil {
		t.Fatalf("first write error = %v", err)
	}

	// Creating again with empty ETag should fail.
	err := WriteMembersFile(context.Background(), store, mf)
	if !errors.Is(err, storage.ErrPrecondition) {
		t.Fatalf("expected ErrPrecondition, got %v", err)
	}
}

func TestAllocateMemberID_Empty(t *testing.T) {
	mf := MembersFile{Members: map[string]uint64{}}

	id := AllocateMemberID(mf)
	if id != 1 {
		t.Fatalf("AllocateMemberID() = %d, want 1", id)
	}
}

func TestAllocateMemberID_Increments(t *testing.T) {
	mf := MembersFile{
		Members: map[string]uint64{
			"node-1": 1,
			"node-2": 2,
			"node-3": 5,
		},
	}

	id := AllocateMemberID(mf)
	if id != 6 {
		t.Fatalf("AllocateMemberID() = %d, want 6", id)
	}
}

func TestFindMemberID_Exists(t *testing.T) {
	mf := MembersFile{
		Members: map[string]uint64{
			"node-1": 12345,
			"node-2": 67890,
		},
	}

	id, ok := FindMemberID(mf, "node-1")
	if !ok {
		t.Fatal("FindMemberID() returned false for existing node")
	}
	if id != 12345 {
		t.Errorf("FindMemberID() = %d, want %d", id, 12345)
	}
}

func TestFindMemberID_NotExists(t *testing.T) {
	mf := MembersFile{
		Members: map[string]uint64{
			"node-1": 12345,
		},
	}

	_, ok := FindMemberID(mf, "node-999")
	if ok {
		t.Fatal("FindMemberID() returned true for non-existent node")
	}
}
