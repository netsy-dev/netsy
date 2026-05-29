// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"bufio"
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"log/slog"

	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/datafile"
	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestRunPreflightPassImportsDurableChunks verifies that Primary preflight
// replays chunk files that are already durable in object storage.
func TestRunPreflightPassImportsDurableChunks(t *testing.T) {
	db := openPrimaryTestDB(t)
	store := storage.NewMemoryStore()
	state := nodestate.New(slog.Default())
	cfg := &config.Config{
		NodeConfig: config.NodeConfig{
			NodeID:  "node-a",
			DataDir: t.TempDir(),
		},
	}

	insertPrimaryReplicatedRecord(t, db, testReplicatedRecord(1, nil))

	srv := newPrimaryPreflightTestServer(t, cfg, db, store, state)
	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatalf("SetPrimary(Starting) error = %v", err)
	}

	record2 := testReplicatedRecord(2, nil)
	key2, data2 := encodePrimaryTestChunk(t, cfg.NodeID, record2)
	if err := store.Put(context.Background(), key2, data2); err != nil {
		t.Fatalf("store.Put(%s) error = %v", key2, err)
	}

	if err := srv.runPreflightPass(context.Background()); err != nil {
		t.Fatalf("runPreflightPass() error = %v", err)
	}

	latestRevision, err := db.LatestRevision()
	if err != nil {
		t.Fatalf("LatestRevision() error = %v", err)
	}
	if latestRevision != 2 {
		t.Fatalf("LatestRevision() = %d, want 2", latestRevision)
	}
	if state.Committed() != 2 {
		t.Fatalf("Committed() = %d, want 2", state.Committed())
	}
	if got := srv.nextRevisionID.Load(); got != 3 {
		t.Fatalf("nextRevisionID = %d, want 3", got)
	}
}

// TestRunPreflightPassUploadsUndurableRecords verifies that Primary preflight
// uploads local records missing from object storage, seeds compaction state,
// and relays records plus the starting commit to connected Replicas.
func TestRunPreflightPassUploadsUndurableRecords(t *testing.T) {
	db := openPrimaryTestDB(t)
	store := storage.NewMemoryStore()
	state := nodestate.New(slog.Default())
	cfg := &config.Config{
		NodeConfig: config.NodeConfig{
			NodeID:  "node-a",
			DataDir: t.TempDir(),
		},
	}

	insertPrimaryReplicatedRecord(t, db, testReplicatedRecord(1, timestamppb.New(time.Unix(10, 0).UTC())))
	insertPrimaryReplicatedRecord(t, db, testReplicatedRecord(2, nil))
	insertPrimaryReplicatedRecord(t, db, testReplicatedRecord(3, nil))

	srv := newPrimaryPreflightTestServer(t, cfg, db, store, state)
	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatalf("SetPrimary(Starting) error = %v", err)
	}

	session := srv.addFollowStream("replica-a")

	if err := srv.runPreflightPass(context.Background()); err != nil {
		t.Fatalf("runPreflightPass() error = %v", err)
	}

	for revision := int64(1); revision <= 3; revision++ {
		if _, _, err := store.Get(context.Background(), datastore.ChunkKey(revision)); err != nil {
			t.Fatalf("store.Get(chunk %d) error = %v", revision, err)
		}
	}

	compactionRevision, err := db.LatestCompactionRevision()
	if err != nil {
		t.Fatalf("LatestCompactionRevision() error = %v", err)
	}
	if compactionRevision != 1 {
		t.Fatalf("LatestCompactionRevision() = %d, want 1", compactionRevision)
	}
	if state.Compaction() != 1 {
		t.Fatalf("Compaction() = %d, want 1", state.Compaction())
	}
	if state.Committed() != 3 {
		t.Fatalf("Committed() = %d, want 3", state.Committed())
	}
	if got := srv.nextRevisionID.Load(); got != 4 {
		t.Fatalf("nextRevisionID = %d, want 4", got)
	}

	assertFollowRecord(t, <-session.sendCh, 1)
	assertFollowRecord(t, <-session.sendCh, 2)
	assertFollowRecord(t, <-session.sendCh, 3)
	assertFollowCommit(t, <-session.sendCh, 3)
}

// TestPrimaryCrashRecoveryViaPreflight verifies that a new Primary elected
// after a crash recovers un-synced records from SQLite and uploads them to
// object storage during preflight.
func TestPrimaryCrashRecoveryViaPreflight(t *testing.T) {
	db := openPrimaryTestDB(t)
	store := storage.NewMemoryStore()
	state := nodestate.New(slog.Default())
	cfg := &config.Config{
		NodeConfig: config.NodeConfig{
			NodeID:  "node-b",
			DataDir: t.TempDir(),
		},
	}

	// Simulate previous Primary: records 1-3 in SQLite, only record 1 in S3.
	insertPrimaryReplicatedRecord(t, db, testReplicatedRecord(1, nil))
	insertPrimaryReplicatedRecord(t, db, testReplicatedRecord(2, nil))
	insertPrimaryReplicatedRecord(t, db, testReplicatedRecord(3, nil))

	key1, data1 := encodePrimaryTestChunk(t, "node-a", testReplicatedRecord(1, nil))
	if err := store.Put(context.Background(), key1, data1); err != nil {
		t.Fatalf("store.Put(%s) error = %v", key1, err)
	}

	// New Primary picks up after crash.
	srv := newPrimaryPreflightTestServer(t, cfg, db, store, state)
	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatalf("SetPrimary(Starting) error = %v", err)
	}

	session := srv.addFollowStream("replica-a")

	if err := srv.runPreflightPass(context.Background()); err != nil {
		t.Fatalf("runPreflightPass() error = %v", err)
	}

	// All 3 records should now be in object storage.
	for revision := int64(1); revision <= 3; revision++ {
		if _, _, err := store.Get(context.Background(), datastore.ChunkKey(revision)); err != nil {
			t.Fatalf("store.Get(chunk %d) error = %v", revision, err)
		}
	}

	if state.Committed() != 3 {
		t.Fatalf("Committed() = %d, want 3", state.Committed())
	}
	if got := srv.nextRevisionID.Load(); got != 4 {
		t.Fatalf("nextRevisionID = %d, want 4", got)
	}

	// Replica should have received records 2 and 3 (record 1 was already durable)
	// and a commit message.
	assertFollowRecord(t, <-session.sendCh, 2)
	assertFollowRecord(t, <-session.sendCh, 3)
	assertFollowCommit(t, <-session.sendCh, 3)
}

// openPrimaryTestDB creates and opens a SQLite database for Primary tests.
func openPrimaryTestDB(t *testing.T) localdb.Database {
	t.Helper()

	db := localdb.New(filepath.Join(t.TempDir(), "primary.sqlite3"))
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

// newPrimaryPreflightTestServer constructs a Primary server with the minimum
// dependencies needed to exercise preflight logic in tests.
func newPrimaryPreflightTestServer(
	t *testing.T,
	cfg *config.Config,
	db localdb.Database,
	store storage.ObjectStorage,
	state *nodestate.State,
) *Server {
	t.Helper()

	srv := &Server{
		logger:        slog.Default(),
		config:        cfg,
		db:            db,
		storageClient: store,
		state:         state,
		replicas:      NewReplicas(),
		followStreams: make(map[string]*followSession),
	}
	if err := srv.initializeRevisionCounter(); err != nil {
		t.Fatalf("initializeRevisionCounter() error = %v", err)
	}

	return srv
}

// insertPrimaryReplicatedRecord writes an authoritative record into the test
// database using the same replication path the production code uses.
func insertPrimaryReplicatedRecord(t *testing.T, db localdb.Database, record *proto.Record) {
	t.Helper()

	if _, err := db.ReplicateRecord(record); err != nil {
		t.Fatalf("ReplicateRecord(%d) error = %v", record.GetRevision(), err)
	}
}

// testReplicatedRecord builds a minimal replicated record suitable for
// preflight tests.
func testReplicatedRecord(revision int64, compactedAt *timestamppb.Timestamp) *proto.Record {
	return &proto.Record{
		Revision:       revision,
		Key:            []byte{byte('a' + revision)},
		Created:        true,
		Version:        1,
		CreateRevision: revision,
		CreatedAt:      timestamppb.New(time.Unix(revision, 0).UTC()),
		CompactedAt:    compactedAt,
		LeaderId:       "leader-1",
		Value:          []byte{byte('0' + revision)},
	}
}

// encodePrimaryTestChunk serializes a single record into the chunk-file format
// used by Primary object-storage writes.
func encodePrimaryTestChunk(t *testing.T, leaderID string, record *proto.Record) (string, []byte) {
	t.Helper()

	buffer := &bytes.Buffer{}
	writer, err := datafile.NewWriter(bufio.NewWriter(buffer), proto.FileKind_KIND_CHUNK, 1, leaderID)
	if err != nil {
		t.Fatalf("NewWriter() error = %v", err)
	}
	if err := writer.Write(record); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	return datastore.ChunkKey(record.GetRevision()), buffer.Bytes()
}

// assertFollowRecord verifies that a follow-stream message carries the expected
// record revision.
func assertFollowRecord(t *testing.T, msg *proto.PrimaryMessage, revision int64) {
	t.Helper()

	record := msg.GetRecord()
	if record == nil {
		t.Fatalf("follow message missing record payload: %#v", msg)
	}
	if record.GetRevision() != revision {
		t.Fatalf("follow record revision = %d, want %d", record.GetRevision(), revision)
	}
}

// assertFollowCommit verifies that a follow-stream message carries the
// expected committed revision.
func assertFollowCommit(t *testing.T, msg *proto.PrimaryMessage, revision int64) {
	t.Helper()

	if got := msg.GetCommit(); got != revision {
		t.Fatalf("follow commit = %d, want %d", got, revision)
	}
}
