// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datafile

import (
	"bufio"
	"bytes"
	"testing"
	"time"

	internaldatafile "github.com/netsy-dev/netsy/internal/datafile"
	pb "github.com/netsy-dev/netsy/internal/proto"
)

func TestSnapshotRoundTrip(t *testing.T) {
	records := []*Record{
		{
			Revision:       1,
			Key:            []byte("/registry/example"),
			Created:        true,
			CreateRevision: 1,
			Version:        1,
			Value:          []byte(`{"kind":"Example"}`),
			CreatedAt:      ptr(time.Now()),
			LeaderID:       "leader-a",
		},
	}

	var buf bytes.Buffer
	if err := WriteSnapshot(&buf, records, "leader-a"); err != nil {
		t.Fatalf("WriteSnapshot error = %v", err)
	}

	got, err := ReadSnapshot(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadSnapshot error = %v", err)
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

func TestReadCompressedSnapshot(t *testing.T) {
	records := []*pb.Record{
		{
			Revision:       1,
			Key:            []byte("/registry/example"),
			Created:        true,
			CreateRevision: 1,
			Version:        1,
			Value:          []byte(`{"kind":"Example"}`),
		},
	}

	var buf bytes.Buffer
	writer, err := internaldatafile.NewWriter(bufio.NewWriter(&buf), pb.FileKind_KIND_SNAPSHOT, int64(len(records)), "leader-a")
	if err != nil {
		t.Fatalf("NewWriter error = %v", err)
	}
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Write error = %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close error = %v", err)
	}

	got, err := ReadSnapshot(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("ReadSnapshot error = %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1", len(got))
	}
	if string(got[0].Value) != string(records[0].Value) {
		t.Fatalf("got value %q, want %q", got[0].Value, records[0].Value)
	}
}

func ptr[T any](value T) *T {
	return &value
}
