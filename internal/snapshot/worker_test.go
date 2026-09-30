// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// openWorkerTestDB creates a real SQLite-backed localdb
func openWorkerTestDB(t *testing.T) localdb.Database {
	t.Helper()

	db := localdb.New(filepath.Join(t.TempDir(), "worker.sqlite3"))
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

// insertWorkerTestRecord writes a minimal committed record at the given
// revision, so FindAllRecordsForSnapshot has something to return
func insertWorkerTestRecord(t *testing.T, db localdb.Database, revision int64) {
	t.Helper()

	record := &proto.Record{
		Revision:       revision,
		Key:            []byte{byte('a' + revision%26)},
		Created:        true,
		Version:        1,
		CreateRevision: revision,
		CreatedAt:      timestamppb.New(time.Unix(revision, 0).UTC()),
		LeaderId:       "leader-1",
		Value:          []byte{byte('0' + revision%10)},
	}
	if _, err := db.ReplicateRecord(record); err != nil {
		t.Fatalf("ReplicateRecord(%d) error = %v", revision, err)
	}
}

// newWorkerConfig returns a Config with a writable DataDir/NodeID (needed
// by createSnapshot's temp-file and datafile-writer logic) plus the given
// cleanup batching settings
func newWorkerConfig(t *testing.T, batchSize int, batchInterval time.Duration) *config.Config {
	t.Helper()
	cfg := testConfig(batchSize, batchInterval)
	cfg.NodeID = "test-node"
	cfg.DataDir = t.TempDir()
	return cfg
}

// slowDeleteStore wraps a MemoryStore and adds a fixed delay to every
// DeleteBatch call, simulating the object-store latency that made chunk
// cleanup expensive in the original bug report.
type slowDeleteStore struct {
	*storage.MemoryStore
	deleteDelay time.Duration
}

func (s *slowDeleteStore) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	time.Sleep(s.deleteDelay)
	return s.MemoryStore.DeleteBatch(ctx, keys)
}

func TestCreateSnapshotDoesNotBlockOnCleanup(t *testing.T) {
	db := openWorkerTestDB(t)
	store := &slowDeleteStore{MemoryStore: storage.NewMemoryStore(), deleteDelay: 50 * time.Millisecond}

	for rev := int64(1); rev <= 20; rev++ {
		insertWorkerTestRecord(t, db, rev)
	}
	seedChunks(t, store, 1, 20)

	cfg := newWorkerConfig(t, 5, 0)
	cleaner := NewCleaner(testLogger(), cfg, store, nil)
	cleaner.Start()
	defer cleaner.Stop()

	w := NewWorker(testLogger(), cfg, db, store, cleaner, nil, nil)

	start := time.Now()
	w.createSnapshot(20)
	elapsed := time.Since(start)

	if elapsed > 150*time.Millisecond {
		t.Fatalf("createSnapshot took %s, expected it to return without waiting on chunk cleanup (which alone takes >=200ms here)", elapsed)
	}

	// Cleanup still completes correctly, just asynchronously.
	waitFor(t, 2*time.Second, func() bool { return cleaner.LastCleaned() == 20 })
}

func TestCreateSnapshotEnqueuesCleanupForCorrectRevision(t *testing.T) {
	db := openWorkerTestDB(t)
	store := storage.NewMemoryStore()

	for rev := int64(1); rev <= 16; rev++ {
		insertWorkerTestRecord(t, db, rev)
	}
	seedChunks(t, store, 1, 16) // chunk 16 is NOT covered by the revision-15 snapshot below

	cfg := newWorkerConfig(t, 250, 0)
	cleaner := NewCleaner(testLogger(), cfg, store, nil)
	cleaner.Start()
	defer cleaner.Stop()

	w := NewWorker(testLogger(), cfg, db, store, cleaner, nil, nil)
	w.createSnapshot(15)

	waitFor(t, 2*time.Second, func() bool { return cleaner.LastCleaned() == 15 })

	for rev := int64(1); rev <= 15; rev++ {
		if _, _, err := store.Get(context.Background(), datastore.ChunkKey(rev)); err == nil {
			t.Errorf("chunk at revision %d should have been cleaned up, but still exists", rev)
		}
	}
	if _, _, err := store.Get(context.Background(), datastore.ChunkKey(16)); err != nil {
		t.Errorf("chunk at revision 16 should NOT have been cleaned up (not covered by the snapshot), got err=%v", err)
	}
}

func TestCreateSnapshotSucceedsWithNilCleaner(t *testing.T) {
	db := openWorkerTestDB(t)
	store := storage.NewMemoryStore()
	insertWorkerTestRecord(t, db, 1)

	cfg := newWorkerConfig(t, 250, 0)
	w := NewWorker(testLogger(), cfg, db, store, nil, nil, nil)

	w.createSnapshot(1) // must not panic

	if _, _, err := store.Get(context.Background(), datastore.SnapshotKey(1)); err != nil {
		t.Fatalf("expected snapshot to be uploaded even with a nil cleaner, got err=%v", err)
	}
}

func TestCreateSnapshotUploadsSnapshotFile(t *testing.T) {
	db := openWorkerTestDB(t)
	store := storage.NewMemoryStore()

	for rev := int64(1); rev <= 5; rev++ {
		insertWorkerTestRecord(t, db, rev)
	}

	cfg := newWorkerConfig(t, 250, 0)
	cleaner := NewCleaner(testLogger(), cfg, store, nil)
	cleaner.Start()
	defer cleaner.Stop()

	w := NewWorker(testLogger(), cfg, db, store, cleaner, nil, nil)
	w.createSnapshot(5)

	data, _, err := store.Get(context.Background(), datastore.SnapshotKey(5))
	if err != nil {
		t.Fatalf("expected snapshot object at %s, got err=%v", datastore.SnapshotKey(5), err)
	}
	if len(data) == 0 {
		t.Fatalf("snapshot object at %s is empty", datastore.SnapshotKey(5))
	}
}
