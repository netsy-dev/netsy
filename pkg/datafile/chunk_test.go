// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datafile

import (
	"bytes"
	"strings"
	"testing"
)

func TestChunkRoundTrip(t *testing.T) {
	records := []*Record{
		{
			Revision:       2,
			Key:            []byte("/registry/example"),
			Created:        true,
			CreateRevision: 2,
			Version:        1,
			Value:          []byte(`{"kind":"Example"}`),
			LeaderID:       "leader-a",
		},
	}

	var buf bytes.Buffer
	if err := WriteChunk(&buf, records, "leader-a"); err != nil {
		t.Fatalf("WriteChunk error = %v", err)
	}

	got, err := ReadChunk(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadChunk error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1", len(got))
	}
	if string(got[0].Key) != string(records[0].Key) {
		t.Fatalf("got key %q, want %q", got[0].Key, records[0].Key)
	}
	if string(got[0].Value) != string(records[0].Value) {
		t.Fatalf("got value %q, want %q", got[0].Value, records[0].Value)
	}
}

func TestReadChunkRejectsSnapshot(t *testing.T) {
	records := []*Record{{Revision: 1, Key: []byte("key"), Value: []byte("value")}}

	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, records, "leader-a"); err != nil {
		t.Fatalf("WriteSnapshot error = %v", err)
	}

	_, err := ReadChunk(bytes.NewReader(buf.Bytes()))
	if err == nil {
		t.Fatalf("ReadChunk succeeded for snapshot file")
	}
	if !strings.Contains(err.Error(), "expected kind mismatch") {
		t.Fatalf("ReadChunk error = %v, want expected kind mismatch", err)
	}
}
