// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"bufio"
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"log/slog"

	"github.com/netsy-dev/netsy/internal/datafile"
	"github.com/netsy-dev/netsy/internal/localdb"
	pb "github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestDownloadAndImportFile imports a chunk file from object storage and
// replays its records into SQLite.
func TestDownloadAndImportFile(t *testing.T) {
	db := openDatastoreTestDB(t)
	store := storage.NewMemoryStore()
	record := &pb.Record{
		Revision:       1,
		Key:            []byte("key"),
		Value:          []byte("value"),
		Created:        true,
		CreateRevision: 1,
		Version:        1,
		CreatedAt:      timestamppb.New(time.Unix(1, 0).UTC()),
		LeaderId:       "leader-1",
	}

	payload := encodeDatastoreTestFile(t, pb.FileKind_KIND_CHUNK, record)
	if err := store.Put(context.Background(), ChunkKey(record.GetRevision()), payload); err != nil {
		t.Fatalf("store.Put() error = %v", err)
	}

	if err := DownloadAndImportFile(
		context.Background(),
		slog.Default(),
		db,
		store,
		ChunkKey(record.GetRevision()),
		pb.FileKind_KIND_CHUNK,
		nil,
	); err != nil {
		t.Fatalf("DownloadAndImportFile() error = %v", err)
	}

	got, err := db.FindRecordByRev(record.GetRevision())
	if err != nil {
		t.Fatalf("FindRecordByRev() error = %v", err)
	}
	if string(got.GetKey()) != string(record.GetKey()) {
		t.Fatalf("FindRecordByRev().Key = %q, want %q", got.GetKey(), record.GetKey())
	}
	if string(got.GetValue()) != string(record.GetValue()) {
		t.Fatalf("FindRecordByRev().Value = %q, want %q", got.GetValue(), record.GetValue())
	}
}

// openDatastoreTestDB opens a SQLite database suitable for datastore tests.
func openDatastoreTestDB(t *testing.T) localdb.Database {
	t.Helper()

	db := localdb.New(filepath.Join(t.TempDir(), "datastore.sqlite3"))
	if err := db.Connect(); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	return db
}

// encodeDatastoreTestFile encodes records into a single Netsy file payload for
// datastore import tests.
func encodeDatastoreTestFile(t *testing.T, kind pb.FileKind, records ...*pb.Record) []byte {
	t.Helper()

	buffer := &bytes.Buffer{}
	writer, err := datafile.NewWriter(bufio.NewWriter(buffer), kind, int64(len(records)), "leader-1")
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	for _, record := range records {
		if err := writer.Write(record); err != nil {
			t.Fatalf("Write() error = %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	return buffer.Bytes()
}
