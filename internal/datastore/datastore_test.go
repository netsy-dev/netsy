// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"testing"
)

func TestParseRevisionFromKey(t *testing.T) {
	tests := []struct {
		key     string
		wantRev int64
		wantOK  bool
	}{
		{"chunks/0001/0000000000000000001.netsy", 1, true},
		{"chunks/0099/0000000000000000099.netsy", 99, true},
		{"snapshots/0000000000000000245.netsy", 245, true},
		{"prefix/chunks/0001/0000000000000000001.netsy", 1, true},
		{"chunks/0001/notanumber.netsy", 0, false},
		{"chunks/0001/0000000000000000001.txt", 0, false},
		{"chunks/0001/0000000000000000001", 0, false},
		{"", 0, false},
		{"0000000000000000042.netsy", 42, true}, // just a filename works
	}

	for _, tt := range tests {
		rev, ok := parseRevisionFromKey(tt.key)
		if ok != tt.wantOK {
			t.Errorf("parseRevisionFromKey(%q): got ok=%v, want %v", tt.key, ok, tt.wantOK)
		}
		if ok && rev != tt.wantRev {
			t.Errorf("parseRevisionFromKey(%q): got rev=%d, want %d", tt.key, rev, tt.wantRev)
		}
	}
}

func TestChunkKey(t *testing.T) {
	tests := []struct {
		revision int64
		want     string
	}{
		{1, "chunks/0001/0000000000000000001.netsy"},
		{99, "chunks/0099/0000000000000000099.netsy"},
		{10000, "chunks/0000/0000000000000010000.netsy"},
		{10001, "chunks/0001/0000000000000010001.netsy"},
		{0, "chunks/0000/0000000000000000000.netsy"},
	}

	for _, tt := range tests {
		got := ChunkKey(tt.revision)
		if got != tt.want {
			t.Errorf("ChunkKey(%d) = %q, want %q", tt.revision, got, tt.want)
		}
	}
}

func TestSnapshotKey(t *testing.T) {
	tests := []struct {
		revision int64
		want     string
	}{
		{1, "snapshots/0000000000000000001.netsy"},
		{245, "snapshots/0000000000000000245.netsy"},
		{0, "snapshots/0000000000000000000.netsy"},
	}

	for _, tt := range tests {
		got := SnapshotKey(tt.revision)
		if got != tt.want {
			t.Errorf("SnapshotKey(%d) = %q, want %q", tt.revision, got, tt.want)
		}
	}
}
