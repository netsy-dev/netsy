// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"log/slog"

	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/datafile"
	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/metrics"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
)

// SnapshotRequest represents a request to potentially create a snapshot
type SnapshotRequest struct {
	Revision   int64
	Timestamp  time.Time
	RecordSize int64
}

// Worker handles snapshot creation in a separate goroutine
type Worker struct {
	logger        *slog.Logger
	config        *config.Config
	db            localdb.Database
	storageClient storage.ObjectStorage

	// Channel for receiving snapshot requests
	requestCh chan SnapshotRequest

	// Snapshot state tracking
	lastSnapshotRevision int64
	lastSnapshotTime     time.Time
	cumulativeSize       int64 // Cumulative size since last snapshot
	stateMutex           sync.Mutex

	// Prevents concurrent snapshot creation
	snapshotMutex sync.Mutex

	metrics        *Metrics
	storageMetrics *metrics.ObjectStorageMetrics

	// Context for shutdown
	ctx    context.Context
	cancel context.CancelFunc
}

// NewWorker creates a new snapshot worker
func NewWorker(logger *slog.Logger, config *config.Config, db localdb.Database, storageClient storage.ObjectStorage, snapshotMetrics *Metrics, storageMetrics *metrics.ObjectStorageMetrics) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		logger:         logger,
		config:         config,
		db:             db,
		storageClient:  storageClient,
		requestCh:      make(chan SnapshotRequest, 100), // Buffered channel to avoid blocking
		metrics:        snapshotMetrics,
		storageMetrics: storageMetrics,
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Start begins the snapshot worker goroutine
func (w *Worker) Start() {
	go w.run()
}

// Stop gracefully shuts down the snapshot worker
func (w *Worker) Stop() {
	w.cancel()
}

// RequestSnapshot sends a snapshot request to the worker
func (w *Worker) RequestSnapshot(revision int64, timestamp time.Time, recordSize int64) {
	req := SnapshotRequest{
		Revision:   revision,
		Timestamp:  timestamp,
		RecordSize: recordSize,
	}

	select {
	case w.requestCh <- req:
		// Request sent successfully
	default:
		// Channel is full, log warning but don't block
		w.logger.Warn("snapshot request channel full, dropping request", "revision", revision)
	}
}

// run is the main worker loop
func (w *Worker) run() {
	w.logger.Info("snapshot worker started")

	for {
		select {
		case <-w.ctx.Done():
			w.logger.Info("snapshot worker stopping")
			return
		case req := <-w.requestCh:
			w.processRequest(req)
		}
	}
}

// processRequest handles a single snapshot request
func (w *Worker) processRequest(req SnapshotRequest) {
	w.stateMutex.Lock()
	// Add this record's size to cumulative size
	w.cumulativeSize += req.RecordSize

	shouldCreate, reason := w.shouldCreateSnapshot(
		req.Revision,
		req.Timestamp,
		w.cumulativeSize,
		w.lastSnapshotRevision,
		w.lastSnapshotTime,
	)

	if shouldCreate {
		// Update state and reset cumulative size
		w.lastSnapshotRevision = req.Revision
		w.lastSnapshotTime = req.Timestamp
		w.cumulativeSize = 0 // Reset after snapshot
	}
	w.stateMutex.Unlock()

	if !shouldCreate {
		return
	}

	w.logger.Info("snapshot thresholds met, creating snapshot",
		"current_revision", req.Revision, "reason", reason)

	w.createSnapshot(req.Revision)
}

// shouldCreateSnapshot determines if a snapshot should be created based on thresholds
// Returns (shouldCreate bool, reason string)
func (w *Worker) shouldCreateSnapshot(currentRevision int64, currentTime time.Time, cumulativeSize int64, lastRevision int64, lastTime time.Time) (bool, string) {
	// Prevent duplicate snapshots - only create if we have new records
	if currentRevision <= lastRevision {
		return false, ""
	}

	// Check record count threshold
	recordsThreshold := w.config.Snapshot.ThresholdRecords
	if recordsThreshold > 0 && (currentRevision-lastRevision) >= recordsThreshold {
		w.logger.Debug("snapshot record threshold reached",
			"current_revision", currentRevision, "last_snapshot_revision", lastRevision,
			"records_since_last", currentRevision-lastRevision, "threshold", recordsThreshold)
		return true, "record_count"
	}

	// Check age threshold
	ageThreshold := w.config.Snapshot.ThresholdAgeMinutes
	if ageThreshold > 0 {
		if lastTime.IsZero() {
			// First snapshot - create immediately if age threshold is enabled
			w.logger.Debug("first snapshot - age threshold enabled", "threshold_minutes", ageThreshold)
			return true, "first_snapshot"
		} else {
			timeSinceLastSnapshot := currentTime.Sub(lastTime)
			if timeSinceLastSnapshot >= time.Duration(ageThreshold)*time.Minute {
				w.logger.Debug("snapshot age threshold reached",
					"time_since_last", timeSinceLastSnapshot, "threshold_minutes", ageThreshold)
				return true, "age"
			}
		}
	}

	// Check size threshold using cumulative size since last snapshot
	sizeThresholdMB := w.config.Snapshot.ThresholdSizeMB
	if sizeThresholdMB > 0 {
		cumulativeSizeMB := cumulativeSize / (1024 * 1024)

		if cumulativeSizeMB >= sizeThresholdMB {
			w.logger.Debug("snapshot size threshold reached",
				"cumulative_size_mb", cumulativeSizeMB, "threshold_mb", sizeThresholdMB)
			return true, "size"
		}
	}

	return false, ""
}

// createSnapshot creates and uploads a snapshot file containing all records up to the specified revision
func (w *Worker) createSnapshot(upToRevision int64) {
	// Acquire snapshot mutex to prevent concurrent snapshot creation
	w.snapshotMutex.Lock()
	defer w.snapshotMutex.Unlock()

	snapshotStart := time.Now()
	w.logger.Info("starting snapshot creation", "up_to_revision", upToRevision)

	// Get all non-compacted records up to the specified revision
	records, err := w.db.FindAllRecordsForSnapshot(upToRevision)
	if err != nil {
		w.logger.Error("failed to get records for snapshot", "error", err)
		w.observeSnapshot("error", snapshotStart)
		return
	}

	if len(records) == 0 {
		w.logger.Warn("no records found for snapshot", "up_to_revision", upToRevision)
		w.observeSnapshot("error", snapshotStart)
		return
	}

	// Create temporary file for snapshot
	tempFile, err := os.CreateTemp(w.config.DataDir, fmt.Sprintf("snapshot_%d_*.netsy", upToRevision))
	if err != nil {
		w.logger.Error("failed to create temporary snapshot file", "error", err)
		w.observeSnapshot("error", snapshotStart)
		return
	}
	tempFilePath := tempFile.Name()
	defer func() {
		tempFile.Close()
		if err := os.Remove(tempFilePath); err != nil && !os.IsNotExist(err) {
			w.logger.Warn("failed to cleanup temporary snapshot file", "file", tempFilePath, "error", err)
		}
	}()

	// Write snapshot using datafile writer
	w.logger.Debug("writing snapshot file", "temp_file", tempFilePath, "records_count", len(records))
	err = w.writeSnapshotFile(tempFile, records, upToRevision)
	if err != nil {
		w.logger.Error("failed to write snapshot file", "temp_file", tempFilePath, "error", err)
		w.observeSnapshot("error", snapshotStart)
		return
	}
	w.logger.Debug("snapshot file written successfully", "temp_file", tempFilePath)

	// Close temp file before upload
	tempFile.Close()

	// Upload snapshot to object storage
	snapshotKey := datastore.SnapshotKey(upToRevision)

	w.logger.Info("uploading snapshot to object storage", "key", snapshotKey, "file_path", tempFilePath)

	var uploadBytes int64
	if fi, statErr := os.Stat(tempFilePath); statErr == nil {
		uploadBytes = fi.Size()
	}
	uploadStart := time.Now()
	err = datastore.Upload(w.ctx, w.storageClient, snapshotKey, tempFilePath)
	if w.storageMetrics != nil {
		w.storageMetrics.ObserveWrite("snapshot", "sync", uploadBytes, time.Since(uploadStart), err)
	}
	if err != nil {
		w.logger.Error("failed to upload snapshot to object storage", "key", snapshotKey, "file_path", tempFilePath, "error", err)
		w.observeSnapshot("error", snapshotStart)
		return
	}

	w.logger.Info("snapshot uploaded to object storage successfully", "revision", upToRevision, "records", len(records), "key", snapshotKey)

	// Start cleanup of old chunk files
	w.logger.Info("starting chunk file cleanup", "up_to_revision", upToRevision)

	// List all chunk files that are covered by the snapshot (revision <= upToRevision)
	chunks, err := datastore.ListChunksForCleanup(w.ctx, w.storageClient, upToRevision)
	if err != nil {
		w.logger.Error("failed to list chunks for cleanup", "error", err)
		return
	}
	deletedCount := 0
	for _, chunk := range chunks {
		err := w.storageClient.Delete(w.ctx, chunk.Key)
		if err != nil {
			w.logger.Warn("failed to delete chunk file", "key", chunk.Key, "error", err)
			continue
		}
		deletedCount++
		w.logger.Debug("deleted chunk file", "key", chunk.Key, "revision", chunk.Revision)
	}

	w.logger.Info("chunk file cleanup completed",
		"up_to_revision", upToRevision, "deleted_chunks", deletedCount)

	w.observeSnapshot("success", snapshotStart)
}

// observeSnapshot records snapshot creation metrics when metrics are configured.
func (w *Worker) observeSnapshot(result string, start time.Time) {
	if w.metrics == nil {
		return
	}
	w.metrics.Creations.WithLabelValues(result).Inc()
	w.metrics.CreateDur.WithLabelValues(result).Observe(time.Since(start).Seconds())
	if result == "success" {
		w.metrics.Age.MarkCreated()
	}
}

// writeSnapshotFile writes records to a snapshot file using the datafile writer
func (w *Worker) writeSnapshotFile(file *os.File, records []*proto.Record, upToRevision int64) error {
	// Create buffered writer
	buffer := bufio.NewWriter(file)
	defer buffer.Flush()

	// Create datafile writer for snapshot
	writer, err := datafile.NewWriter(buffer, proto.FileKind_KIND_SNAPSHOT, int64(len(records)), w.config.NodeID)
	if err != nil {
		return fmt.Errorf("failed to create datafile writer: %w", err)
	}

	// Write all records
	for _, record := range records {
		err = writer.Write(record)
		if err != nil {
			return fmt.Errorf("failed to write record %d to snapshot: %w", record.Revision, err)
		}
	}

	// Close writer
	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close datafile writer: %w", err)
	}

	return nil
}

// InitializeWithSnapshot initializes the snapshot worker state from existing snapshot info
func (w *Worker) InitializeWithSnapshot(snapshotInfo *datastore.LatestSnapshotInfo) {
	w.stateMutex.Lock()
	defer w.stateMutex.Unlock()

	if snapshotInfo == nil || !snapshotInfo.Found {
		// No existing snapshots, initialize with default state
		w.lastSnapshotRevision = 0
		w.lastSnapshotTime = time.Time{}
		w.cumulativeSize = 0

		w.logger.Info("no existing snapshots found, initialized with default state")
		return
	}

	// Initialize from existing snapshot
	w.lastSnapshotRevision = snapshotInfo.Revision
	w.lastSnapshotTime = time.Now() // Use current time since we don't know exact creation time
	w.cumulativeSize = 0            // Start tracking from zero

	w.logger.Info("initialized snapshot tracking from existing snapshot",
		"latest_snapshot_revision", snapshotInfo.Revision)
}
