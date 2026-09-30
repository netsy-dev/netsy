// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/storage"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func testConfig(batchSize int, batchInterval time.Duration) *config.Config {
	cfg := &config.Config{}
	cfg.Snapshot.CleanupBatchSize = batchSize
	cfg.Snapshot.CleanupBatchInterval = config.Duration{Duration: batchInterval}
	return cfg
}

func seedChunks(t *testing.T, store storage.ObjectStorage, from, to int64) {
	t.Helper()
	for rev := from; rev <= to; rev++ {
		if err := store.Put(context.Background(), datastore.ChunkKey(rev), []byte("x")); err != nil {
			t.Fatalf("seed chunk %d: %v", rev, err)
		}
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

// blockingStore wraps a MemoryStore and blocks the first call to List
// until the test closes release, letting a test deterministically enqueue
// more work while a cleanup run is in progress.
type blockingStore struct {
	*storage.MemoryStore
	release chan struct{}
	started chan struct{}
	calls   atomic.Int32
}

func newBlockingStore() *blockingStore {
	return &blockingStore{
		MemoryStore: storage.NewMemoryStore(),
		release:     make(chan struct{}),
		started:     make(chan struct{}),
	}
}

func (b *blockingStore) List(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	if b.calls.Add(1) == 1 {
		close(b.started)
		<-b.release
	}
	return b.MemoryStore.List(ctx, prefix)
}

// recordingStore wraps a MemoryStore and records the key-set and timestamp
// of every DeleteBatch call, so a test can assert on batch size and pacing.
type recordingStore struct {
	*storage.MemoryStore
	mu      sync.Mutex
	batches [][]string
	times   []time.Time
}

func newRecordingStore() *recordingStore {
	return &recordingStore{MemoryStore: storage.NewMemoryStore()}
}

func (r *recordingStore) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	r.mu.Lock()
	r.batches = append(r.batches, append([]string(nil), keys...))
	r.times = append(r.times, time.Now())
	r.mu.Unlock()
	return r.MemoryStore.DeleteBatch(ctx, keys)
}

func (r *recordingStore) snapshot() (batches [][]string, times []time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]string(nil), r.batches...), append([]time.Time(nil), r.times...)
}

// flakyStore wraps a MemoryStore and lets a test make DeleteBatch fail for
// specific keys exactly once, to exercise Cleaner's retry-on-failure path.
type flakyStore struct {
	*storage.MemoryStore
	mu       sync.Mutex
	failOnce map[string]bool
}

func newFlakyStore() *flakyStore {
	return &flakyStore{MemoryStore: storage.NewMemoryStore(), failOnce: make(map[string]bool)}
}

func (f *flakyStore) failKeyOnce(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failOnce[key] = true
}

func (f *flakyStore) DeleteBatch(ctx context.Context, keys []string) ([]string, error) {
	f.mu.Lock()
	var toFail, toDelete []string
	for _, key := range keys {
		if f.failOnce[key] {
			toFail = append(toFail, key)
			delete(f.failOnce, key)
		} else {
			toDelete = append(toDelete, key)
		}
	}
	f.mu.Unlock()

	if _, err := f.MemoryStore.DeleteBatch(ctx, toDelete); err != nil {
		return nil, err
	}
	return toFail, nil
}

func TestCleanerCoalescesRapidEnqueues(t *testing.T) {
	store := newBlockingStore()
	cfg := testConfig(250, 0)
	c := NewCleaner(testLogger(), cfg, store, nil)
	c.Start()
	defer c.Stop()

	c.Enqueue(100)
	<-store.started

	for _, rev := range []int64{200, 150, 500, 300} {
		c.Enqueue(rev)
	}
	close(store.release)

	waitFor(t, time.Second, func() bool { return c.LastCleaned() == 500 })

	if got := store.calls.Load(); got != 2 {
		t.Fatalf("expected exactly 2 cleanup runs (List calls), got %d", got)
	}
}

func TestCleanerBoundedBatchingAndRateLimiting(t *testing.T) {
	store := newRecordingStore()
	seedChunks(t, store, 1, 10)

	batchInterval := 20 * time.Millisecond
	cfg := testConfig(3, batchInterval)
	c := NewCleaner(testLogger(), cfg, store, nil)
	c.Start()
	defer c.Stop()

	c.Enqueue(10)
	waitFor(t, time.Second, func() bool { return c.LastCleaned() == 10 })

	batches, times := store.snapshot()
	wantSizes := []int{3, 3, 3, 1}
	if len(batches) != len(wantSizes) {
		t.Fatalf("expected %d batches, got %d: %v", len(wantSizes), len(batches), batches)
	}
	for i, want := range wantSizes {
		if len(batches[i]) != want {
			t.Errorf("batch %d: expected %d keys, got %d", i, want, len(batches[i]))
		}
	}

	for i := 1; i < len(times); i++ {
		gap := times[i].Sub(times[i-1])
		if gap < batchInterval/2 {
			t.Errorf("batch %d fired only %s after batch %d, expected roughly %s", i, gap, i-1, batchInterval)
		}
	}
}

func TestCleanerStopsPromptlyMidCleanup(t *testing.T) {
	store := storage.NewMemoryStore()
	seedChunks(t, store, 1, 200)

	cfg := testConfig(1, 50*time.Millisecond)
	c := NewCleaner(testLogger(), cfg, store, nil)
	c.Start()
	c.Enqueue(200)

	time.Sleep(20 * time.Millisecond)
	stopStart := time.Now()
	c.Stop()
	elapsed := time.Since(stopStart)

	if elapsed > 500*time.Millisecond {
		t.Fatalf("Stop() took %s, expected prompt interruption of an in-progress run", elapsed)
	}
}

func TestCleanerRetriesFailedChunksOnNextEnqueue(t *testing.T) {
	store := newFlakyStore()
	seedChunks(t, store, 1, 2)
	store.failKeyOnce(datastore.ChunkKey(1))

	cfg := testConfig(250, 0)
	c := NewCleaner(testLogger(), cfg, store, nil)
	c.Start()
	defer c.Stop()

	c.Enqueue(2)

	// Chunk 2 deletes fine, chunk 1 fails — so this run is partial and
	// lastCleaned must not advance past it.
	waitFor(t, time.Second, func() bool {
		_, _, err := store.Get(context.Background(), datastore.ChunkKey(2))
		return errors.Is(err, storage.ErrNotFound)
	})
	if got := c.LastCleaned(); got == 2 {
		t.Fatalf("LastCleaned() = %d, expected it to stay below 2 after a partial run", got)
	}
	if _, _, err := store.Get(context.Background(), datastore.ChunkKey(1)); err != nil {
		t.Fatalf("chunk at revision 1 was expected to still exist after its failed delete, got err=%v", err)
	}

	// A higher revision retries everything not yet cleaned, including
	// the still-present chunk from revision 1 — this time it succeeds.
	seedChunks(t, store, 3, 3)
	c.Enqueue(3)
	waitFor(t, time.Second, func() bool { return c.LastCleaned() == 3 })

	for _, rev := range []int64{1, 2, 3} {
		if _, _, err := store.Get(context.Background(), datastore.ChunkKey(rev)); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("chunk at revision %d should be deleted after the retry, got err=%v", rev, err)
		}
	}
}
func TestCleanerStaleEnqueueIsNoOp(t *testing.T) {
	store := newRecordingStore()
	seedChunks(t, store, 1, 5)

	cfg := testConfig(250, 0)
	c := NewCleaner(testLogger(), cfg, store, nil)
	c.Start()
	defer c.Stop()

	c.Enqueue(5)
	waitFor(t, time.Second, func() bool { return c.LastCleaned() == 5 })

	batchesBefore, _ := store.snapshot()

	c.Enqueue(5)
	c.Enqueue(3)
	time.Sleep(50 * time.Millisecond)

	batchesAfter, _ := store.snapshot()
	if len(batchesAfter) != len(batchesBefore) {
		t.Fatalf("expected no additional cleanup runs for stale revisions, got %d new batches", len(batchesAfter)-len(batchesBefore))
	}
}

func TestCleanerDoesNotAdvanceLastCleanedOnFailure(t *testing.T) {
	store := newFlakyStore()
	seedChunks(t, store, 1, 5)
	store.failKeyOnce(datastore.ChunkKey(3))

	cfg := testConfig(250, 0)
	c := NewCleaner(testLogger(), cfg, store, nil)
	c.Start()
	defer c.Stop()

	c.Enqueue(5)

	// Wait for the run to settle: chunks 1,2,4,5 gone, chunk 3 still present.
	waitFor(t, time.Second, func() bool {
		_, _, err3 := store.Get(context.Background(), datastore.ChunkKey(3))
		_, _, err5 := store.Get(context.Background(), datastore.ChunkKey(5))
		return errors.Is(err5, storage.ErrNotFound) && err3 == nil
	})

	if got := c.LastCleaned(); got == 5 {
		t.Fatalf("LastCleaned() = %d, expected it to stay below 5 since a chunk failed to delete", got)
	}

	// Re-enqueuing the SAME revision must not be treated as stale now —
	// it should retry and pick up the previously-failed chunk.
	c.Enqueue(5)
	waitFor(t, time.Second, func() bool { return c.LastCleaned() == 5 })

	if _, _, err := store.Get(context.Background(), datastore.ChunkKey(3)); !errors.Is(err, storage.ErrNotFound) {
		t.Errorf("chunk at revision 3 should be deleted after retry, got err=%v", err)
	}
}

func TestCleanerInitializeWithSnapshotSetsStartingPoint(t *testing.T) {
	c := NewCleaner(testLogger(), testConfig(250, 0), storage.NewMemoryStore(), nil)
	c.InitializeWithSnapshot(&datastore.LatestSnapshotInfo{Found: true, Revision: 42})

	if got := c.LastCleaned(); got != 42 {
		t.Fatalf("LastCleaned() = %d, want 42", got)
	}
}

func TestCleanerDirectStrategyDeletesWithoutListing(t *testing.T) {
	store := newRecordingStore()
	seedChunks(t, store, 1, 5)

	cfg := testConfig(250, 0)
	cfg.Snapshot.CleanupStrategy = "direct"
	c := NewCleaner(testLogger(), cfg, store, nil)
	c.Start()
	defer c.Stop()

	c.Enqueue(5)
	waitFor(t, time.Second, func() bool { return c.LastCleaned() == 5 })

	batches, _ := store.snapshot()
	if len(batches) != 1 || len(batches[0]) != 5 {
		t.Fatalf("expected one batch of 5 computed keys, got %v", batches)
	}
	for _, rev := range []int64{1, 2, 3, 4, 5} {
		if _, _, err := store.Get(context.Background(), datastore.ChunkKey(rev)); !errors.Is(err, storage.ErrNotFound) {
			t.Errorf("chunk at revision %d should be deleted, got err=%v", rev, err)
		}
	}
}
