// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package watch

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/netsy-dev/netsy/internal/proto"
)

func TestIsWatchMatch(t *testing.T) {
	tests := []struct {
		w      watchEntry
		record *proto.Record
		expect bool
	}{
		// different key
		{watchEntry{key: []byte("1")}, &proto.Record{Key: []byte("")}, false},
		{watchEntry{key: []byte("1")}, &proto.Record{Key: []byte("")}, false},
		{watchEntry{key: []byte("1")}, &proto.Record{Key: []byte("")}, false},
		// exact match
		{watchEntry{key: []byte("1")}, &proto.Record{Key: []byte("1")}, true},
		{watchEntry{key: []byte("1")}, &proto.Record{Key: []byte("1")}, true},
		{watchEntry{key: []byte("1")}, &proto.Record{Key: []byte("1")}, true},
		// 1 inside range 1-3
		{watchEntry{key: []byte("1"), rangeEnd: []byte("3")}, &proto.Record{Key: []byte("1")}, true},
		{watchEntry{key: []byte("1"), rangeEnd: []byte("3")}, &proto.Record{Key: []byte("1")}, true},
		{watchEntry{key: []byte("1"), rangeEnd: []byte("3")}, &proto.Record{Key: []byte("1")}, true},
		// 2 inside range 1-3
		{watchEntry{key: []byte("1"), rangeEnd: []byte("3")}, &proto.Record{Key: []byte("2")}, true},
		{watchEntry{key: []byte("1"), rangeEnd: []byte("3")}, &proto.Record{Key: []byte("2")}, true},
		{watchEntry{key: []byte("1"), rangeEnd: []byte("3")}, &proto.Record{Key: []byte("2")}, true},
		// 1 prefix match (range 1-2 triggers prefix match)
		{watchEntry{key: []byte("1"), rangeEnd: []byte("2")}, &proto.Record{Key: []byte("1")}, true},
		{watchEntry{key: []byte("1"), rangeEnd: []byte("2")}, &proto.Record{Key: []byte("1")}, true},
		{watchEntry{key: []byte("1"), rangeEnd: []byte("2")}, &proto.Record{Key: []byte("1")}, true},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			result := isWatchMatch(test.w, test.record)
			if result != test.expect {
				t.Errorf("isWatchMatch(%+v, %+v)\n= %t\nwant %t", test.w, test.record, result, test.expect)
			}
		})
	}
}

func TestSetWatchAdmissionFloorNoWatches(t *testing.T) {
	m := NewManager(slog.Default(), nil)

	if err := m.SetWatchAdmissionFloor(50); err != nil {
		t.Fatalf("SetWatchAdmissionFloor(50) error = %v", err)
	}
	if got := m.WatchAdmissionFloor(); got != 50 {
		t.Fatalf("WatchAdmissionFloor() = %d, want 50", got)
	}
}

func TestSetWatchAdmissionFloorRejectsWhenWatchBelow(t *testing.T) {
	m := NewManager(slog.Default(), nil)

	// Simulate a watcher with a watch at startRevision 30.
	w := &Watcher{
		id:       1,
		watches:  map[int64]watchEntry{1: {startRevision: 30}},
		progress: map[int64]bool{},
	}
	m.Register(w)

	err := m.SetWatchAdmissionFloor(50)
	if err == nil {
		t.Fatal("SetWatchAdmissionFloor(50) should fail with active watch at revision 30")
	}

	// Floor should have been rolled back.
	if got := m.WatchAdmissionFloor(); got != 0 {
		t.Fatalf("WatchAdmissionFloor() = %d, want 0 after rollback", got)
	}
}

func TestSetWatchAdmissionFloorAcceptsWhenWatchAbove(t *testing.T) {
	m := NewManager(slog.Default(), nil)

	w := &Watcher{
		id:       1,
		watches:  map[int64]watchEntry{1: {startRevision: 80}},
		progress: map[int64]bool{},
	}
	m.Register(w)

	if err := m.SetWatchAdmissionFloor(50); err != nil {
		t.Fatalf("SetWatchAdmissionFloor(50) error = %v", err)
	}
	if got := m.WatchAdmissionFloor(); got != 50 {
		t.Fatalf("WatchAdmissionFloor() = %d, want 50", got)
	}
}

func TestSetWatchAdmissionFloorOverwritesPrevious(t *testing.T) {
	m := NewManager(slog.Default(), nil)

	if err := m.SetWatchAdmissionFloor(50); err != nil {
		t.Fatalf("SetWatchAdmissionFloor(50) error = %v", err)
	}

	// A second set with a lower value (rollback) should overwrite.
	if err := m.SetWatchAdmissionFloor(30); err != nil {
		t.Fatalf("SetWatchAdmissionFloor(30) error = %v", err)
	}
	if got := m.WatchAdmissionFloor(); got != 30 {
		t.Fatalf("WatchAdmissionFloor() = %d, want 30", got)
	}
}

// TestCompactionNoticeRejectsActiveWatch verifies the compaction notice
// atomicity protocol: SetWatchAdmissionFloor fails when an active watch
// exists below the proposed floor, rolls back on rejection, and succeeds
// after the conflicting watch is removed.
func TestCompactionNoticeRejectsActiveWatch(t *testing.T) {
	m := NewManager(slog.Default(), nil)

	// Register a watcher with a watch at startRevision 5.
	w := &Watcher{
		id:       1,
		watches:  map[int64]watchEntry{100: {startRevision: 5}},
		progress: map[int64]bool{},
	}
	m.Register(w)

	// Step 1: SetWatchAdmissionFloor(10) should fail — watch at rev 5 is below 10.
	if err := m.SetWatchAdmissionFloor(10); err == nil {
		t.Fatal("SetWatchAdmissionFloor(10) should fail with active watch at rev 5")
	}

	// Floor should have been rolled back to 0.
	if got := m.WatchAdmissionFloor(); got != 0 {
		t.Fatalf("WatchAdmissionFloor() = %d, want 0 after rollback", got)
	}

	// Step 2: Remove the conflicting watch.
	w.Lock()
	delete(w.watches, 100)
	w.Unlock()

	// Step 3: SetWatchAdmissionFloor(10) should succeed now.
	if err := m.SetWatchAdmissionFloor(10); err != nil {
		t.Fatalf("SetWatchAdmissionFloor(10) after watch removal error = %v", err)
	}
	if got := m.WatchAdmissionFloor(); got != 10 {
		t.Fatalf("WatchAdmissionFloor() = %d, want 10", got)
	}

	// Step 4: A new watch at rev 8 (below floor 10) should be blocked.
	// Verify by adding it to a new watcher and trying SetWatchAdmissionFloor again.
	w2 := &Watcher{
		id:       2,
		watches:  map[int64]watchEntry{200: {startRevision: 8}},
		progress: map[int64]bool{},
	}
	m.Register(w2)

	// Floor is already 10. Setting it to 10 again should fail because
	// the new watch at rev 8 is below 10.
	if err := m.SetWatchAdmissionFloor(10); err == nil {
		t.Fatal("SetWatchAdmissionFloor(10) should fail with new watch at rev 8")
	}

	// But a watch at rev 15 (above floor) is fine.
	w3 := &Watcher{
		id:       3,
		watches:  map[int64]watchEntry{300: {startRevision: 15}},
		progress: map[int64]bool{},
	}
	m.Register(w3)

	// Remove the conflicting w2 watch.
	w2.Lock()
	delete(w2.watches, 200)
	w2.Unlock()

	// Now SetWatchAdmissionFloor(12) should succeed — only w3 (rev 15) remains.
	if err := m.SetWatchAdmissionFloor(12); err != nil {
		t.Fatalf("SetWatchAdmissionFloor(12) error = %v", err)
	}
	if got := m.WatchAdmissionFloor(); got != 12 {
		t.Fatalf("WatchAdmissionFloor() = %d, want 12", got)
	}
}
