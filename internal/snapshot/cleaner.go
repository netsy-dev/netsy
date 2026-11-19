// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/storage"
)

// Cleaner runs chunk cleanup independently of snapshot creation, on its
// own goroutine.
//
// A snapshot at revision S covers every chunk named at or below S, but on
// the quorum path some of those chunks may not be written yet when the
// snapshot fires. The Cleaner therefore never cleans past its ceiling, the
// highest revision at which no further chunk can be written, and resumes
// when Wake reports the ceiling may have moved. Each run covers only
// (lastCleaned, min(snapshot, ceiling)], so every key is visited once.
type Cleaner struct {
	logger        *slog.Logger
	config        *config.Config
	storageClient storage.ObjectStorage
	metrics       *Metrics

	wake chan struct{} // buffered(1): non-blocking "there's new work" signal

	mu              sync.Mutex
	pendingRevision int64        // highest snapshot revision requested for cleanup
	lastCleaned     int64        // highest revision fully cleaned so far
	ceiling         func() int64 // nil means no ceiling beyond the snapshot
	swept           bool         // a list-based run has succeeded since the last reset
	retryDelay      time.Duration
	retryTimer      *time.Timer // pending retry of a failed run, guarded by mu
	ctx             context.Context
	cancel          context.CancelFunc
	done            chan struct{}
	started         atomic.Bool
}

// NewCleaner creates a new chunk cleanup worker.
func NewCleaner(logger *slog.Logger, cfg *config.Config, storageClient storage.ObjectStorage, snapshotMetrics *Metrics) *Cleaner {
	ctx, cancel := context.WithCancel(context.Background())
	return &Cleaner{
		logger:        logger,
		config:        cfg,
		storageClient: storageClient,
		metrics:       snapshotMetrics,
		wake:          make(chan struct{}, 1),
		retryDelay:    time.Minute,
		ctx:           ctx,
		cancel:        cancel,
		done:          make(chan struct{}),
	}
}

// Start begins the cleaner goroutine
func (c *Cleaner) Start() {
	c.started.Store(true)
	go c.run()
}

// Stop signals the cleaner to shut down and waits for it to exit. Safe to
// call even if Start was never invoked
func (c *Cleaner) Stop() {
	c.cancel()
	c.mu.Lock()
	if c.retryTimer != nil {
		c.retryTimer.Stop()
	}
	c.mu.Unlock()
	if c.started.Load() {
		<-c.done
	}
}

// SetCeiling sets the function that reports the highest revision at which
// no further chunk can be written. Cleanup never goes past it. The function
// is called without the Cleaner's lock held.
func (c *Cleaner) SetCeiling(ceiling func() int64) {
	c.mu.Lock()
	c.ceiling = ceiling
	c.mu.Unlock()
}

// Wake asks the cleaner to re-check for work, for example because the
// ceiling may have advanced. Safe to call from any goroutine.
func (c *Cleaner) Wake() {
	select {
	case c.wake <- struct{}{}:
	default:
		// A wake is already pending; the loop will read the latest state
		// when it next runs.
	}
}

// ResetSweep makes the next direct-mode run list the bucket first, so
// chunks left behind by a crash or by another Primary are found.
func (c *Cleaner) ResetSweep() {
	c.mu.Lock()
	c.swept = false
	c.mu.Unlock()
}

// Enqueue requests cleanup of all chunks covered by upToRevision. Safe to
// call from any goroutine.
func (c *Cleaner) Enqueue(upToRevision int64) {
	c.mu.Lock()
	if upToRevision <= c.lastCleaned {
		c.mu.Unlock()
		return
	}
	if upToRevision > c.pendingRevision {
		c.pendingRevision = upToRevision
		if c.metrics != nil {
			c.metrics.CleanupQueuedRevision.Set(float64(upToRevision))
		}
	}
	c.mu.Unlock()

	c.Wake()
}

// LastCleaned returns the highest revision fully cleaned so far.
func (c *Cleaner) LastCleaned() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastCleaned
}

// run is the main cleaner loop. Each wake triggers at most one run, so a
// run that fails is not repeated until a retry, Enqueue or Wake arrives.
func (c *Cleaner) run() {
	defer close(c.done)
	c.logger.Info("chunk cleanup worker started")

	for {
		select {
		case <-c.ctx.Done():
			c.logger.Info("chunk cleanup worker stopping")
			return
		case <-c.wake:
			target, ok := c.nextTarget()
			if !ok {
				continue
			}
			c.performCleanup(target)
			if c.ctx.Err() != nil {
				return
			}
		}
	}
}

// nextTarget returns the revision the next run should clean through: the
// pending snapshot revision, capped at the ceiling. It reports false when
// there is nothing to clean yet.
func (c *Cleaner) nextTarget() (target int64, ok bool) {
	c.mu.Lock()
	pending := c.pendingRevision
	lastCleaned := c.lastCleaned
	ceiling := c.ceiling
	c.mu.Unlock()

	if pending <= lastCleaned {
		return 0, false
	}

	target = pending
	if ceiling != nil {
		durable := ceiling()
		if c.metrics != nil {
			c.metrics.CleanupCeilingRevision.Set(float64(durable))
		}
		if durable < target {
			target = durable
		}
	}
	if target <= lastCleaned {
		c.logger.Debug("chunk cleanup waiting for chunks to be written",
			"pending_revision", pending, "cleaned_through", lastCleaned, "ceiling", target)
		return 0, false
	}
	return target, true
}

// InitializeWithSnapshot sets the cleaner's starting point from the last
// known snapshot, so a restart does not start cleanup from revision 0.
func (c *Cleaner) InitializeWithSnapshot(snapshotInfo *datastore.LatestSnapshotInfo) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if snapshotInfo == nil || !snapshotInfo.Found {
		return
	}
	c.lastCleaned = snapshotInfo.Revision
	if c.metrics != nil {
		c.metrics.CleanupLastRevision.Set(float64(snapshotInfo.Revision))
	}
}

// performCleanup deletes, in bounded rate-limited batches, the chunk
// objects covered by upToRevision. In list mode it asks the store what
// exists first (safe on versioned buckets). In direct mode it computes
// every revision key above lastCleaned and deletes them without listing,
// which is cheaper on backends where listing costs more than deleting.
// The first direct-mode run after a reset lists instead, to catch chunks
// left behind outside that range.
//
// It never runs on the snapshot goroutine, so snapshot upload is never
// blocked by it. lastCleaned only advances when nothing failed; a failed
// run is retried after retryDelay.
func (c *Cleaner) performCleanup(upToRevision int64) {
	start := time.Now()
	strategy := c.resolveStrategy()

	c.mu.Lock()
	from := c.lastCleaned + 1
	pending := c.pendingRevision
	sweep := strategy == "direct" && !c.swept
	c.mu.Unlock()

	if sweep {
		strategy = "list"
	}
	c.logger.Info("chunk cleanup starting",
		"up_to_revision", upToRevision,
		"pending_revision", pending,
		"strategy", strategy,
		"startup_sweep", sweep,
	)

	var keys []string
	switch strategy {
	case "direct":
		for rev := from; rev <= upToRevision; rev++ {
			keys = append(keys, datastore.ChunkKey(rev))
		}
		if c.metrics != nil {
			c.metrics.ChunksRequested.Add(float64(len(keys)))
		}
	default: // list
		chunks, err := datastore.ListChunksForCleanup(c.ctx, c.storageClient, upToRevision)
		if err != nil {
			c.logger.Error("chunk cleanup: failed to list chunks", "up_to_revision", upToRevision, "error", err)
			c.observeCleanup("error", start)
			c.scheduleRetry(upToRevision)
			return
		}
		keys = make([]string, len(chunks))
		for i, chunk := range chunks {
			keys[i] = chunk.Key
		}
		if c.metrics != nil {
			c.metrics.ChunksListed.Add(float64(len(keys)))
		}
	}

	batchSize := c.config.Snapshot.CleanupBatchSize
	if batchSize <= 0 {
		batchSize = 250
	}
	batchInterval := c.config.Snapshot.CleanupBatchInterval.Duration

	succeeded := 0
	failed := 0

	for i := 0; i < len(keys); i += batchSize {
		if c.ctx.Err() != nil {
			c.logger.Info("chunk cleanup interrupted by shutdown",
				"up_to_revision", upToRevision, "succeeded", succeeded, "candidates", len(keys))
			c.observeCleanup("interrupted", start)
			return
		}

		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		batch := keys[i:end]

		failedKeys, err := c.storageClient.DeleteBatch(c.ctx, batch)
		if err != nil {
			c.logger.Warn("chunk cleanup: delete batch failed", "batch_size", len(batch), "error", err)
			failed += len(batch)
			if c.metrics != nil {
				c.metrics.ChunksFailed.Add(float64(len(batch)))
			}
		} else {
			failedSet := make(map[string]struct{}, len(failedKeys))
			for _, k := range failedKeys {
				failedSet[k] = struct{}{}
			}
			for _, key := range batch {
				if _, isFailed := failedSet[key]; isFailed {
					failed++
					if c.metrics != nil {
						c.metrics.ChunksFailed.Inc()
					}
					continue
				}
				succeeded++
				if c.metrics != nil && strategy != "direct" {
					c.metrics.ChunksDeleted.Inc()
				}
			}
		}

		if end < len(keys) && batchInterval > 0 {
			select {
			case <-c.ctx.Done():
				c.logger.Info("chunk cleanup interrupted by shutdown",
					"up_to_revision", upToRevision, "succeeded", succeeded, "candidates", len(keys))
				c.observeCleanup("interrupted", start)
				return
			case <-time.After(batchInterval):
			}
		}
	}

	c.mu.Lock()
	if failed == 0 && upToRevision > c.lastCleaned {
		c.lastCleaned = upToRevision
		c.swept = true
	}
	cleanedThrough := c.lastCleaned
	c.mu.Unlock()

	if c.metrics != nil {
		c.metrics.CleanupLastRevision.Set(float64(cleanedThrough))
	}

	result := "success"
	if failed > 0 {
		result = "partial"
	}

	c.logger.Info("chunk cleanup completed",
		"up_to_revision", upToRevision,
		"cleaned_through", cleanedThrough,
		"strategy", strategy,
		"candidates", len(keys),
		"succeeded", succeeded,
		"failed", failed,
		"duration", time.Since(start),
	)

	c.observeCleanup(result, start)
	if failed > 0 {
		c.scheduleRetry(upToRevision)
	}
}

// scheduleRetry wakes the cleaner again after retryDelay, so a failed run
// gets another try even if no new snapshot arrives. The pending revision is
// kept until it is fully cleaned, so the retry picks it up. Only one retry
// is pending at a time.
func (c *Cleaner) scheduleRetry(upToRevision int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.retryTimer != nil {
		c.retryTimer.Stop()
	}
	c.retryTimer = time.AfterFunc(c.retryDelay, func() {
		if c.ctx.Err() == nil {
			c.Wake()
		}
	})
	c.logger.Info("chunk cleanup retry scheduled", "up_to_revision", upToRevision, "delay", c.retryDelay)
}

// observeCleanup records cleanup run metrics when metrics are configured.
func (c *Cleaner) observeCleanup(result string, start time.Time) {
	if c.metrics == nil {
		return
	}
	c.metrics.CleanupRuns.WithLabelValues(result).Inc()
	c.metrics.CleanupDur.WithLabelValues(result).Observe(time.Since(start).Seconds())
}

// resolveStrategy turns cleanup_strategy into list or direct. Auto picks
// direct when the backend supports native bulk delete, else list.
func (c *Cleaner) resolveStrategy() string {
	switch c.config.Snapshot.CleanupStrategy {
	case "direct":
		return "direct"
	case "list":
		return "list"
	default:
		if c.storageClient.BulkDeleteLimit() > 0 {
			return "direct"
		}
		return "list"
	}
}
