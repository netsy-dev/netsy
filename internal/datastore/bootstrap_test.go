// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"bufio"
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/datafile"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestPromoteBootstrapSnapshotCreatesNormalSnapshot(t *testing.T) {
	store := storage.NewMemoryStore()
	data := marshalBootstrapTestSnapshot(t, []*proto.Record{
		bootstrapTestRecord(1),
		bootstrapTestRecord(2),
		bootstrapTestRecord(3),
	})
	if err := store.Put(context.Background(), BootstrapSnapshotKey, data); err != nil {
		t.Fatalf("Put(%s) error = %v", BootstrapSnapshotKey, err)
	}

	info, err := PromoteBootstrapSnapshot(context.Background(), store, t.TempDir())
	if err != nil {
		t.Fatalf("PromoteBootstrapSnapshot() error = %v", err)
	}
	if !info.Found {
		t.Fatal("PromoteBootstrapSnapshot() Found=false, want true")
	}
	if info.Revision != 3 {
		t.Fatalf("PromoteBootstrapSnapshot() Revision = %d, want 3", info.Revision)
	}
	if info.Key != SnapshotKey(3) {
		t.Fatalf("PromoteBootstrapSnapshot() Key = %q, want %q", info.Key, SnapshotKey(3))
	}

	promoted, _, err := store.Get(context.Background(), SnapshotKey(3))
	if err != nil {
		t.Fatalf("Get(promoted snapshot) error = %v", err)
	}
	if !bytes.Equal(promoted, data) {
		t.Fatal("promoted snapshot data differs from bootstrap snapshot data")
	}
}

func TestPromoteBootstrapSnapshotIsIdempotentAfterSnapshotCreated(t *testing.T) {
	store := storage.NewMemoryStore()
	data := marshalBootstrapTestSnapshot(t, []*proto.Record{bootstrapTestRecord(1)})
	if err := store.Put(context.Background(), BootstrapSnapshotKey, data); err != nil {
		t.Fatalf("Put(%s) error = %v", BootstrapSnapshotKey, err)
	}

	if _, err := PromoteBootstrapSnapshot(context.Background(), store, t.TempDir()); err != nil {
		t.Fatalf("first PromoteBootstrapSnapshot() error = %v", err)
	}
	if _, err := PromoteBootstrapSnapshot(context.Background(), store, t.TempDir()); err != nil {
		t.Fatalf("second PromoteBootstrapSnapshot() error = %v", err)
	}
}

func TestPromoteBootstrapSnapshotRejectsDifferentExistingTargetSnapshot(t *testing.T) {
	store := storage.NewMemoryStore()
	data := marshalBootstrapTestSnapshot(t, []*proto.Record{bootstrapTestRecord(1)})
	if err := store.Put(context.Background(), BootstrapSnapshotKey, data); err != nil {
		t.Fatalf("Put(%s) error = %v", BootstrapSnapshotKey, err)
	}

	differentRecord := bootstrapTestRecord(1)
	differentRecord.Value = []byte("different")
	differentData := marshalBootstrapTestSnapshot(t, []*proto.Record{differentRecord})
	if err := store.Put(context.Background(), SnapshotKey(1), differentData); err != nil {
		t.Fatalf("Put(existing target snapshot) error = %v", err)
	}

	if _, err := PromoteBootstrapSnapshot(context.Background(), store, t.TempDir()); err == nil {
		t.Fatal("PromoteBootstrapSnapshot() succeeded with different existing target snapshot")
	}
}

func TestPromoteBootstrapSnapshotRejectsNormalHistory(t *testing.T) {
	tests := []struct {
		name       string
		putHistory func(t *testing.T, store storage.ObjectStorage)
	}{
		{
			name: "different snapshot",
			putHistory: func(t *testing.T, store storage.ObjectStorage) {
				t.Helper()
				data := marshalBootstrapTestSnapshot(t, []*proto.Record{bootstrapTestRecord(1), bootstrapTestRecord(2)})
				if err := store.Put(context.Background(), SnapshotKey(2), data); err != nil {
					t.Fatalf("Put(snapshot) error = %v", err)
				}
			},
		},
		{
			name: "chunk",
			putHistory: func(t *testing.T, store storage.ObjectStorage) {
				t.Helper()
				key, data, err := MarshalChunk(bootstrapTestRecord(2), "node-a")
				if err != nil {
					t.Fatalf("MarshalChunk() error = %v", err)
				}
				if err := store.Put(context.Background(), key, data); err != nil {
					t.Fatalf("Put(chunk) error = %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := storage.NewMemoryStore()
			data := marshalBootstrapTestSnapshot(t, []*proto.Record{bootstrapTestRecord(1)})
			if err := store.Put(context.Background(), BootstrapSnapshotKey, data); err != nil {
				t.Fatalf("Put(%s) error = %v", BootstrapSnapshotKey, err)
			}
			tt.putHistory(t, store)

			if _, err := PromoteBootstrapSnapshot(context.Background(), store, t.TempDir()); err == nil {
				t.Fatal("PromoteBootstrapSnapshot() succeeded with existing normal history")
			}
		})
	}
}

func TestPromoteBootstrapSnapshotRejectsInvalidFiles(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "wrong kind",
			data: marshalBootstrapTestChunk(t, bootstrapTestRecord(1)),
		},
		{
			name: "gap",
			data: marshalBootstrapTestSnapshot(t, []*proto.Record{bootstrapTestRecord(1), bootstrapTestRecord(3)}),
		},
		{
			name: "missing initial record",
			data: marshalBootstrapTestSnapshot(t, []*proto.Record{bootstrapTestRecordWithKey(1, "not-netsy")}),
		},
		{
			name: "empty",
			data: marshalBootstrapTestSnapshot(t, nil),
		},
		{
			name: "corrupt",
			data: []byte("not a netsy file"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := storage.NewMemoryStore()
			if err := store.Put(context.Background(), BootstrapSnapshotKey, tt.data); err != nil {
				t.Fatalf("Put(%s) error = %v", BootstrapSnapshotKey, err)
			}

			if _, err := PromoteBootstrapSnapshot(context.Background(), store, t.TempDir()); err == nil {
				t.Fatal("PromoteBootstrapSnapshot() succeeded with invalid bootstrap snapshot")
			}
		})
	}
}

func marshalBootstrapTestSnapshot(t *testing.T, records []*proto.Record) []byte {
	t.Helper()

	var buf bytes.Buffer
	writer, err := datafile.NewWriter(bufio.NewWriter(&buf), proto.FileKind_KIND_SNAPSHOT, int64(len(records)), "node-a")
	if err != nil {
		t.Fatalf("NewWriter(snapshot) error = %v", err)
	}
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Write(snapshot record %d) error = %v", record.GetRevision(), err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close(snapshot writer) error = %v", err)
	}

	return buf.Bytes()
}

func marshalBootstrapTestChunk(t *testing.T, record *proto.Record) []byte {
	t.Helper()

	var buf bytes.Buffer
	writer, err := datafile.NewWriter(bufio.NewWriter(&buf), proto.FileKind_KIND_CHUNK, 1, "node-a")
	if err != nil {
		t.Fatalf("NewWriter(chunk) error = %v", err)
	}
	if err := writer.Write(record); err != nil {
		t.Fatalf("Write(chunk record %d) error = %v", record.GetRevision(), err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close(chunk writer) error = %v", err)
	}

	return buf.Bytes()
}

func bootstrapTestRecord(revision int64) *proto.Record {
	key := string([]byte{byte('a' + revision)})
	if revision == 1 {
		key = bootstrapInitialRecordKey
	}
	return bootstrapTestRecordWithKey(revision, key)
}

func bootstrapTestRecordWithKey(revision int64, key string) *proto.Record {
	return &proto.Record{
		Revision:       revision,
		Key:            []byte(key),
		Created:        true,
		Version:        1,
		CreateRevision: revision,
		CreatedAt:      timestamppb.New(time.Unix(revision, 0).UTC()),
		LeaderId:       "leader-1",
		Value:          []byte{byte('0' + revision)},
	}
}
