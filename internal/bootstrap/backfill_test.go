// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"bufio"
	"bytes"
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/datafile"
	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestBackfillChunksOnly verifies that backfillChunksFromRevision correctly
// imports chunk files into an empty local SQLite database when no snapshot
// exists, producing a contiguous revision sequence.
func TestBackfillChunksOnly(t *testing.T) {
	db := openTestDB(t)
	store := storage.NewMemoryStore()
	cfg := &config.Config{
		NodeConfig: config.NodeConfig{
			NodeID:  "node-a",
			DataDir: t.TempDir(),
		},
	}

	// Pre-load object storage with 3 chunk files.
	for i := int64(1); i <= 3; i++ {
		record := testRecord(i)
		key, data, err := datastore.MarshalChunk(record, cfg.NodeID)
		if err != nil {
			t.Fatalf("MarshalChunk(%d) error = %v", i, err)
		}
		if err := store.Put(context.Background(), key, data); err != nil {
			t.Fatalf("store.Put(%s) error = %v", key, err)
		}
	}

	if err := backfillChunksFromRevision(
		context.Background(),
		slog.Default(),
		db,
		0,
		store,
		nil,
	); err != nil {
		t.Fatalf("backfillChunksFromRevision() error = %v", err)
	}

	latestRevision, err := db.LatestRevision()
	if err != nil {
		t.Fatalf("LatestRevision() error = %v", err)
	}
	if latestRevision != 3 {
		t.Fatalf("LatestRevision() = %d, want 3", latestRevision)
	}
	if err := db.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity() error = %v", err)
	}
}

// TestBackfillFromSnapshotAndChunks verifies the full bootstrap replay
// path: a snapshot covering revisions 1–5 is imported first, then chunk
// files for revisions 6–8 are backfilled, producing a contiguous
// revision sequence with no gaps or duplicates.
func TestBackfillFromSnapshotAndChunks(t *testing.T) {
	db := openTestDB(t)
	store := storage.NewMemoryStore()
	cfg := &config.Config{
		NodeConfig: config.NodeConfig{
			NodeID:  "node-a",
			DataDir: t.TempDir(),
		},
	}

	// Pre-load a snapshot covering revisions 1-5.
	snapshotRecords := make([]*proto.Record, 5)
	for i := int64(1); i <= 5; i++ {
		snapshotRecords[i-1] = testRecord(i)
	}
	snapshotKey, snapshotData := marshalTestSnapshot(t, cfg.NodeID, snapshotRecords)
	if err := store.Put(context.Background(), snapshotKey, snapshotData); err != nil {
		t.Fatalf("store.Put(snapshot) error = %v", err)
	}

	snapshotInfo := &datastore.LatestSnapshotInfo{
		Revision: 5,
		Key:      snapshotKey,
		Size:     int64(len(snapshotData)),
		Found:    true,
	}

	// Pre-load chunk files for revisions 6-8.
	for i := int64(6); i <= 8; i++ {
		record := testRecord(i)
		key, data, err := datastore.MarshalChunk(record, cfg.NodeID)
		if err != nil {
			t.Fatalf("MarshalChunk(%d) error = %v", i, err)
		}
		if err := store.Put(context.Background(), key, data); err != nil {
			t.Fatalf("store.Put(%s) error = %v", key, err)
		}
	}

	// Step 1: Import the snapshot (covers revisions 1-5).
	if err := importLatestSnapshot(
		context.Background(),
		slog.Default(),
		db,
		snapshotInfo,
		store,
		nil,
	); err != nil {
		t.Fatalf("importLatestSnapshot() error = %v", err)
	}

	latestAfterSnapshot, err := db.LatestRevision()
	if err != nil {
		t.Fatalf("LatestRevision() after snapshot error = %v", err)
	}
	if latestAfterSnapshot != 5 {
		t.Fatalf("LatestRevision() after snapshot = %d, want 5", latestAfterSnapshot)
	}

	// Step 2: Backfill chunks above the snapshot revision (6-8).
	if err := backfillChunksFromRevision(
		context.Background(),
		slog.Default(),
		db,
		5,
		store,
		nil,
	); err != nil {
		t.Fatalf("backfillChunksFromRevision() error = %v", err)
	}

	// Verify all 8 records imported.
	latestRevision, err := db.LatestRevision()
	if err != nil {
		t.Fatalf("LatestRevision() error = %v", err)
	}
	if latestRevision != 8 {
		t.Fatalf("LatestRevision() = %d, want 8", latestRevision)
	}

	// Verify no gaps.
	if err := db.VerifyIntegrity(); err != nil {
		t.Fatalf("VerifyIntegrity() error = %v", err)
	}
}

// openTestDB creates and opens a SQLite database for bootstrap tests.
func openTestDB(t *testing.T) localdb.Database {
	t.Helper()

	db := localdb.New(filepath.Join(t.TempDir(), "test.sqlite3"))
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

// testRecord builds a minimal record suitable for backfill tests.
func testRecord(revision int64) *proto.Record {
	return &proto.Record{
		Revision:       revision,
		Key:            []byte{byte('a' + revision)},
		Created:        true,
		Version:        1,
		CreateRevision: revision,
		CreatedAt:      timestamppb.New(time.Unix(revision, 0).UTC()),
		LeaderId:       "leader-1",
		Value:          []byte{byte('0' + revision)},
	}
}

// marshalTestSnapshot serialises records into the snapshot datafile format,
// returning the object-storage key and raw bytes.
func marshalTestSnapshot(t *testing.T, nodeID string, records []*proto.Record) (string, []byte) {
	t.Helper()

	var buf bytes.Buffer
	w, err := datafile.NewWriter(bufio.NewWriter(&buf), proto.FileKind_KIND_SNAPSHOT, int64(len(records)), nodeID)
	if err != nil {
		t.Fatalf("NewWriter(snapshot) error = %v", err)
	}
	for _, r := range records {
		if err := w.Write(r); err != nil {
			t.Fatalf("Write(rev %d) error = %v", r.Revision, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	lastRevision := records[len(records)-1].GetRevision()
	return datastore.SnapshotKey(lastRevision), buf.Bytes()
}
