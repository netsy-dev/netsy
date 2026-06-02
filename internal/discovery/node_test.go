// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"context"
	"testing"

	"github.com/netsy-dev/netsy/internal/storage"
)

func TestNodePath(t *testing.T) {
	got := NodePath("node-1")
	want := "nodes/node-1.json"
	if got != want {
		t.Errorf("NodePath(\"node-1\") = %q, want %q", got, want)
	}
}

func TestWriteNodeRegistration_New(t *testing.T) {
	store := storage.NewMemoryStore()
	reg := NodeRegistration{
		NodeID:                 "node-1",
		ClientAdvertiseAddress: "https://client:2379",
		PeerAdvertiseAddress:   "https://peer:2380",
	}

	if err := WriteNodeRegistration(context.Background(), store, reg); err != nil {
		t.Fatalf("WriteNodeRegistration() error = %v", err)
	}

	got, err := ReadNodeRegistration(context.Background(), store, "node-1")
	if err != nil {
		t.Fatalf("ReadNodeRegistration() error = %v", err)
	}
	if got != reg {
		t.Fatalf("ReadNodeRegistration() = %+v, want %+v", got, reg)
	}
}

func TestWriteNodeRegistration_Equivalent(t *testing.T) {
	store := storage.NewMemoryStore()
	reg := NodeRegistration{
		NodeID:                 "node-1",
		ClientAdvertiseAddress: "https://client:2379",
		PeerAdvertiseAddress:   "https://peer:2380",
	}

	if err := WriteNodeRegistration(context.Background(), store, reg); err != nil {
		t.Fatalf("first write error = %v", err)
	}

	// Writing the same registration again should be a no-op.
	if err := WriteNodeRegistration(context.Background(), store, reg); err != nil {
		t.Fatalf("second write (equivalent) error = %v", err)
	}
}

func TestWriteNodeRegistration_DifferentAddresses(t *testing.T) {
	store := storage.NewMemoryStore()
	reg := NodeRegistration{
		NodeID:                 "node-1",
		ClientAdvertiseAddress: "https://client:2379",
		PeerAdvertiseAddress:   "https://peer:2380",
	}

	if err := WriteNodeRegistration(context.Background(), store, reg); err != nil {
		t.Fatalf("first write error = %v", err)
	}

	reg.ClientAdvertiseAddress = "https://different:2379"
	err := WriteNodeRegistration(context.Background(), store, reg)
	if err == nil {
		t.Fatal("expected error when writing registration with different addresses")
	}
}

func TestReadNodeRegistration_NotFound(t *testing.T) {
	store := storage.NewMemoryStore()

	_, err := ReadNodeRegistration(context.Background(), store, "missing")
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestListNodeRegistrations(t *testing.T) {
	store := storage.NewMemoryStore()

	reg1 := NodeRegistration{NodeID: "node-1", ClientAdvertiseAddress: "a", PeerAdvertiseAddress: "b"}
	reg2 := NodeRegistration{NodeID: "node-2", ClientAdvertiseAddress: "c", PeerAdvertiseAddress: "d"}

	if err := WriteNodeRegistration(context.Background(), store, reg1); err != nil {
		t.Fatal(err)
	}
	if err := WriteNodeRegistration(context.Background(), store, reg2); err != nil {
		t.Fatal(err)
	}

	// Add a malformed entry that should be skipped.
	_ = store.Put(context.Background(), "nodes/bad.json", []byte("not json"))

	regs, err := ListNodeRegistrations(context.Background(), store)
	if err != nil {
		t.Fatalf("ListNodeRegistrations() error = %v", err)
	}
	if len(regs) != 2 {
		t.Fatalf("expected 2 registrations, got %d", len(regs))
	}
}

func TestDeleteNodeRegistration(t *testing.T) {
	store := storage.NewMemoryStore()
	reg := NodeRegistration{NodeID: "node-1", ClientAdvertiseAddress: "a", PeerAdvertiseAddress: "b"}

	if err := WriteNodeRegistration(context.Background(), store, reg); err != nil {
		t.Fatal(err)
	}
	if err := DeleteNodeRegistration(context.Background(), store, "node-1"); err != nil {
		t.Fatal(err)
	}
	_, err := ReadNodeRegistration(context.Background(), store, "node-1")
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}
