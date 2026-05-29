// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/metrics"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
	googlepb "google.golang.org/protobuf/proto"
)

// chunkBufferLoopInterval is the cadence used to evaluate age-triggered and
// Draining-triggered flushes.
const chunkBufferLoopInterval = 100 * time.Millisecond

// chunkBufferState captures the Primary state transitions the chunk buffer
// needs in order to trigger Draining on persistent flush pressure.
type chunkBufferState interface {
	Primary() nodestate.PrimaryState
	SetPrimary(nodestate.PrimaryState) error
}

// chunkBuffer holds committed records that still need to be flushed to object
// storage as a single chunk file.
type chunkBuffer struct {
	logger         *slog.Logger
	state          chunkBufferState
	storageClient  storage.ObjectStorage
	leaderID       string
	mu             sync.Mutex
	records        []*proto.Record
	bytes          int64
	oldestAt       time.Time
	flushing       bool
	thresholdBytes int64
	thresholdAge   time.Duration
	metrics        *Metrics
	storageMetrics *metrics.ObjectStorageMetrics
}

func newChunkBuffer(
	logger *slog.Logger,
	state chunkBufferState,
	storageClient storage.ObjectStorage,
	leaderID string,
	thresholdSizeMB int,
	thresholdAgeMinutes int,
	storageMetrics *metrics.ObjectStorageMetrics,
) *chunkBuffer {
	return &chunkBuffer{
		logger:         logger,
		state:          state,
		storageClient:  storageClient,
		leaderID:       leaderID,
		thresholdBytes: int64(thresholdSizeMB) * 1024 * 1024,
		thresholdAge:   time.Duration(thresholdAgeMinutes) * time.Minute,
		storageMetrics: storageMetrics,
	}
}

// bufferRecord appends a committed record to the in-memory chunk buffer and
// flushes immediately when the size threshold is reached.
func (b *chunkBuffer) bufferRecord(ctx context.Context, record *proto.Record) error {
	if record == nil {
		return fmt.Errorf("buffer record: nil record")
	}

	b.mu.Lock()
	if len(b.records) == 0 {
		b.oldestAt = time.Now()
	}
	b.records = append(b.records, record)
	b.bytes += int64(googlepb.Size(record))
	full := b.thresholdBytes > 0 && b.bytes >= b.thresholdBytes
	if b.metrics != nil {
		b.metrics.ChunkBufferRecords.Set(float64(len(b.records)))
		b.metrics.ChunkBufferBytes.Set(float64(b.bytes))
	}
	b.mu.Unlock()

	if !full {
		return nil
	}

	if err := b.flush(ctx, "size"); err != nil {
		b.mu.Lock()
		stillFull := b.thresholdBytes > 0 && b.bytes >= b.thresholdBytes
		b.mu.Unlock()
		if stillFull {
			b.transitionToDraining(err)
		}
		return err
	}

	return nil
}

// setMetrics sets the Primary metrics reference used by flush to record
// chunk buffer flush counters and histograms.
func (b *chunkBuffer) setMetrics(m *Metrics) {
	b.mu.Lock()
	b.metrics = m
	b.mu.Unlock()
	if m != nil {
		m.ChunkBufferAge.SetFunc(func() float64 {
			b.mu.Lock()
			defer b.mu.Unlock()
			if len(b.records) > 0 && !b.oldestAt.IsZero() {
				return time.Since(b.oldestAt).Seconds()
			}
			return 0
		})
	}
}

// flush uploads the current chunk buffer as one chunk file. When a flush is
// already in progress or the buffer is empty, it returns immediately.
func (b *chunkBuffer) flush(ctx context.Context, trigger string) error {
	b.mu.Lock()
	if len(b.records) == 0 || b.flushing {
		b.mu.Unlock()
		return nil
	}
	b.flushing = true
	records := make([]*proto.Record, len(b.records))
	copy(records, b.records)
	batchBytes := b.bytes
	b.mu.Unlock()

	firstRevision := records[0].GetRevision()
	lastRevision := records[len(records)-1].GetRevision()

	flushStart := time.Now()
	b.logger.Info("chunk_buffer_flush_started",
		"trigger", trigger,
		"records", len(records),
		"bytes", batchBytes,
		"first_revision", firstRevision,
		"last_revision", lastRevision,
	)

	key, data, err := datastore.MarshalChunkBatch(records, b.leaderID)
	if err != nil {
		b.mu.Lock()
		b.flushing = false
		b.mu.Unlock()
		return err
	}
	uploadStart := time.Now()
	err = storage.PutIfAbsent(ctx, b.storageClient, key, data)
	if b.storageMetrics != nil {
		b.storageMetrics.ObserveWrite("chunk", "async", int64(len(data)), time.Since(uploadStart), err)
	}

	b.mu.Lock()
	b.flushing = false
	if err == nil {
		b.records = b.records[len(records):]
		b.bytes -= batchBytes
		if b.bytes < 0 {
			b.bytes = 0
		}
		if len(b.records) == 0 {
			b.oldestAt = time.Time{}
		}
		if b.metrics != nil {
			b.metrics.ChunkBufferRecords.Set(float64(len(b.records)))
			b.metrics.ChunkBufferBytes.Set(float64(b.bytes))
		}
	}
	b.mu.Unlock()

	flushDuration := time.Since(flushStart).Seconds()
	if err != nil {
		if b.metrics != nil {
			b.metrics.ChunkBufferFlushes.WithLabelValues(trigger, "error").Inc()
			b.metrics.ChunkBufferFlushDur.WithLabelValues(trigger, "error").Observe(flushDuration)
		}
		b.logger.Warn("chunk_buffer_flush_completed",
			"trigger", trigger,
			"result", "error",
			"records", len(records),
			"bytes", batchBytes,
			"first_revision", firstRevision,
			"last_revision", lastRevision,
			"duration_ms", int64(flushDuration*1000),
			"error", err,
		)
		return err
	}

	if b.metrics != nil {
		b.metrics.ChunkBufferFlushes.WithLabelValues(trigger, "success").Inc()
		b.metrics.ChunkBufferFlushDur.WithLabelValues(trigger, "success").Observe(flushDuration)
		b.metrics.ObjectStorageRevision.Set(float64(lastRevision))
	}
	b.logger.Info("chunk_buffer_flush_completed",
		"trigger", trigger,
		"result", "success",
		"records", len(records),
		"bytes", batchBytes,
		"first_revision", firstRevision,
		"last_revision", lastRevision,
		"duration_ms", int64(flushDuration*1000),
	)

	return nil
}

// Run evaluates age-triggered and Draining-triggered chunk-buffer flushes
// until ctx is cancelled.
func (b *chunkBuffer) Run(ctx context.Context) {
	ticker := time.NewTicker(chunkBufferLoopInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if b.state.Primary() == nodestate.PrimaryDraining {
			if err := b.flush(ctx, "draining"); err != nil {
				b.logger.Warn("draining chunk buffer flush failed", "error", err)
			}
			continue
		}

		b.mu.Lock()
		shouldFlush := len(b.records) > 0 && b.thresholdAge > 0 &&
			!b.oldestAt.IsZero() && time.Since(b.oldestAt) >= b.thresholdAge
		b.mu.Unlock()

		if shouldFlush {
			if err := b.flush(ctx, "age"); err != nil {
				b.logger.Warn("age-triggered chunk buffer flush failed", "error", err)
			}
		}
	}
}

// transitionToDrainingForChunkBuffer moves the Primary into Draining when a
// full chunk buffer cannot be flushed.
func (b *chunkBuffer) transitionToDraining(cause error) {
	if b.state.Primary() != nodestate.PrimaryActive {
		return
	}
	if err := b.state.SetPrimary(nodestate.PrimaryDraining); err != nil {
		b.logger.Error("failed to transition primary to draining after chunk buffer flush failure",
			"error", err,
			"cause", cause,
		)
		return
	}
	b.logger.Warn("primary transitioned to draining because the full chunk buffer could not be flushed",
		"error", cause,
	)
}
