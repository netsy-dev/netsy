// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/datafile"
	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
	googlepb "google.golang.org/protobuf/proto"
)

// failingPutStore forces PutIfMatch to fail while delegating the rest of the
// object-storage interface to MemoryStore.
type failingPutStore struct {
	*storage.MemoryStore
	err error
}

// PutIfMatch returns the configured write error for chunk-buffer failure tests.
func (s *failingPutStore) PutIfMatch(_ context.Context, _ string, _ []byte, _ string) error {
	return s.err
}

// newChunkBufferTestServer constructs a Primary server with only the
// dependencies needed for chunk-buffer tests.
func newChunkBufferTestServer(t *testing.T, store storage.ObjectStorage) *Server {
	t.Helper()

	state := nodestate.New(slog.Default())
	if err := state.SetPrimary(nodestate.PrimaryStarting); err != nil {
		t.Fatalf("SetPrimary(Starting) error = %v", err)
	}
	if err := state.SetPrimary(nodestate.PrimaryActive); err != nil {
		t.Fatalf("SetPrimary(Active) error = %v", err)
	}

	return &Server{
		logger: slog.Default(),
		state:  state,
		chunkBuffer: newChunkBuffer(
			slog.Default(),
			state,
			store,
			"node-a",
			0,
			0,
			nil,
		),
	}
}

// TestBufferRecordFlushesOnSizeThreshold verifies that reaching the size
// threshold uploads one chunk containing the buffered records.
func TestBufferRecordFlushesOnSizeThreshold(t *testing.T) {
	store := storage.NewMemoryStore()
	srv := newChunkBufferTestServer(t, store)

	record1 := testReplicatedRecord(1, nil)
	record2 := testReplicatedRecord(2, nil)
	srv.chunkBuffer.thresholdBytes = int64(googlepb.Size(record1) + googlepb.Size(record2))

	if err := srv.chunkBuffer.bufferRecord(context.Background(), record1); err != nil {
		t.Fatalf("bufferRecord(record1) error = %v", err)
	}
	if err := srv.chunkBuffer.bufferRecord(context.Background(), record2); err != nil {
		t.Fatalf("bufferRecord(record2) error = %v", err)
	}

	data, _, err := store.Get(context.Background(), datastore.ChunkKey(2))
	if err != nil {
		t.Fatalf("Get(flushed chunk) error = %v", err)
	}

	records := decodeChunkRecords(t, data)
	if len(records) != 2 {
		t.Fatalf("decoded %d records, want 2", len(records))
	}
	if records[0].GetRevision() != 1 || records[1].GetRevision() != 2 {
		t.Fatalf("decoded revisions = [%d %d], want [1 2]", records[0].GetRevision(), records[1].GetRevision())
	}

	if got := len(srv.chunkBuffer.records); got != 0 {
		t.Fatalf("buffered records after flush = %d, want 0", got)
	}
}

// TestRunChunkBufferLoopFlushesOnAgeThreshold verifies that the background
// chunk-buffer loop flushes aged records.
func TestRunChunkBufferLoopFlushesOnAgeThreshold(t *testing.T) {
	store := storage.NewMemoryStore()
	srv := newChunkBufferTestServer(t, store)
	srv.chunkBuffer.thresholdBytes = 1 << 20
	srv.chunkBuffer.thresholdAge = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go srv.chunkBuffer.Run(ctx)

	record := testReplicatedRecord(7, nil)
	if err := srv.chunkBuffer.bufferRecord(context.Background(), record); err != nil {
		t.Fatalf("bufferRecord() error = %v", err)
	}

	waitForChunk(t, store, datastore.ChunkKey(7))
}

// TestRunChunkBufferLoopFlushesOnDraining verifies that entering Draining
// triggers an immediate flush of buffered records.
func TestRunChunkBufferLoopFlushesOnDraining(t *testing.T) {
	store := storage.NewMemoryStore()
	srv := newChunkBufferTestServer(t, store)
	srv.chunkBuffer.thresholdBytes = 1 << 20
	srv.chunkBuffer.thresholdAge = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go srv.chunkBuffer.Run(ctx)

	record := testReplicatedRecord(9, nil)
	if err := srv.chunkBuffer.bufferRecord(context.Background(), record); err != nil {
		t.Fatalf("bufferRecord() error = %v", err)
	}

	if err := srv.state.SetPrimary(nodestate.PrimaryDraining); err != nil {
		t.Fatalf("SetPrimary(Draining) error = %v", err)
	}

	waitForChunk(t, store, datastore.ChunkKey(9))
}

// TestBufferRecordTransitionsToDrainingWhenFullFlushFails verifies that a full
// buffer moves the Primary into Draining when the flush attempt fails.
func TestBufferRecordTransitionsToDrainingWhenFullFlushFails(t *testing.T) {
	store := &failingPutStore{
		MemoryStore: storage.NewMemoryStore(),
		err:         errors.New("upload failed"),
	}
	srv := newChunkBufferTestServer(t, store)
	srv.chunkBuffer.thresholdBytes = 1

	record := testReplicatedRecord(11, nil)
	err := srv.chunkBuffer.bufferRecord(context.Background(), record)
	if err == nil {
		t.Fatal("bufferRecord() error = nil, want flush failure")
	}
	if srv.state.Primary() != nodestate.PrimaryDraining {
		t.Fatalf("Primary() = %s, want %s", srv.state.Primary(), nodestate.PrimaryDraining)
	}
	if got := len(srv.chunkBuffer.records); got != 1 {
		t.Fatalf("buffered records after failed flush = %d, want 1", got)
	}
}

// marshalFailStore wraps a MemoryStore and fails uploads with a specific error
// the first N times, then delegates normally.
type marshalFailStore struct {
	*storage.MemoryStore
	failCount int
	calls     int
}

func (s *marshalFailStore) PutIfMatch(ctx context.Context, key string, data []byte, etag string) error {
	s.calls++
	if s.calls <= s.failCount {
		return errors.New("simulated upload failure")
	}
	return s.MemoryStore.PutIfMatch(ctx, key, data, etag)
}

// TestFlushRecoversAfterUploadError verifies that a failed upload resets the
// flushing flag so subsequent flushes are not permanently blocked.
func TestFlushRecoversAfterUploadError(t *testing.T) {
	store := &marshalFailStore{MemoryStore: storage.NewMemoryStore(), failCount: 1}
	srv := newChunkBufferTestServer(t, store)

	record := testReplicatedRecord(42, nil)
	srv.chunkBuffer.mu.Lock()
	srv.chunkBuffer.records = append(srv.chunkBuffer.records, record)
	srv.chunkBuffer.bytes = int64(googlepb.Size(record))
	srv.chunkBuffer.mu.Unlock()

	// First flush fails at the upload stage.
	err := srv.chunkBuffer.flush(context.Background(), "test")
	if err == nil {
		t.Fatal("flush() error = nil, want upload failure")
	}

	// The critical assertion: flushing must be reset so future flushes work.
	srv.chunkBuffer.mu.Lock()
	stuck := srv.chunkBuffer.flushing
	srv.chunkBuffer.mu.Unlock()
	if stuck {
		t.Fatal("flushing flag stuck true after upload error")
	}

	// Prove recovery: second flush should succeed.
	if err := srv.chunkBuffer.flush(context.Background(), "retry"); err != nil {
		t.Fatalf("flush() after recovery error = %v", err)
	}
	if _, _, err := store.Get(context.Background(), datastore.ChunkKey(42)); err != nil {
		t.Fatalf("recovered flush did not write chunk: %v", err)
	}
}

// decodeChunkRecords reads every record from an encoded chunk file.
func decodeChunkRecords(t *testing.T, data []byte) []*proto.Record {
	t.Helper()

	kind := proto.FileKind_KIND_CHUNK
	reader, err := datafile.NewReader(bufio.NewReader(bytes.NewReader(data)), &kind)
	if err != nil {
		t.Fatalf("NewReader() error = %v", err)
	}

	records := make([]*proto.Record, 0, reader.Count())
	for i := int64(0); i < reader.Count(); i++ {
		record, err := reader.Read()
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
		records = append(records, record)
	}
	if _, err := reader.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	return records
}

// waitForChunk blocks until the expected chunk appears in object storage or
// the test times out.
func waitForChunk(t *testing.T, store storage.ObjectStorage, key string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, err := store.Get(context.Background(), key); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("chunk %s was not flushed before timeout", key)
}
