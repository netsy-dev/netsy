// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/netsy-dev/netsy/internal/datafile"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/metrics"
	pb "github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
)

// Download retrieves an object from storage, using a temp file for large
// objects to avoid holding everything in memory.
func Download(ctx context.Context, store storage.ObjectStorage, key string, size int64, dataDir string, tempFiles *[]string) (io.ReadCloser, error) {
	const maxMemorySize = 2 * 1024 * 1024 // 2MB

	reader, err := store.GetStream(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to download file: %w", err)
	}

	if size <= maxMemorySize {
		return reader, nil
	}

	// For large files, write to a temp file to avoid holding everything in memory
	var prefix string
	if strings.Contains(key, "snapshots/") {
		prefix = "snapshot_"
	} else {
		prefix = "chunk_"
	}

	tempFile, err := os.CreateTemp(dataDir, prefix+"*.netsy")
	if err != nil {
		reader.Close()
		return nil, fmt.Errorf("failed to create temporary file: %w", err)
	}
	tempPath := tempFile.Name()
	*tempFiles = append(*tempFiles, tempPath)

	if _, err := io.Copy(tempFile, reader); err != nil {
		tempFile.Close()
		reader.Close()
		return nil, fmt.Errorf("failed to write to temporary file: %w", err)
	}
	reader.Close()
	tempFile.Close()

	readFile, err := os.Open(tempPath)
	if err != nil {
		return nil, fmt.Errorf("failed to reopen downloaded file: %w", err)
	}
	return readFile, nil
}

// DownloadAndImportFile downloads a Netsy chunk or snapshot file from object
// storage and replays its records into SQLite.
func DownloadAndImportFile(
	ctx context.Context,
	logger *slog.Logger,
	db localdb.Database,
	store storage.ObjectStorage,
	dataDir string,
	key string,
	size int64,
	expectedKind pb.FileKind,
	storageMetrics *metrics.ObjectStorageMetrics,
) error {
	var tempFiles []string
	defer cleanupTempFiles(logger, tempFiles)

	readStart := time.Now()
	reader, err := Download(ctx, store, key, size, dataDir, &tempFiles)
	if storageMetrics != nil {
		storageMetrics.ObserveRead(time.Since(readStart), err)
	}
	if err != nil {
		return fmt.Errorf("download %s: %w", key, err)
	}
	defer reader.Close()

	dataReader, err := datafile.NewReader(bufio.NewReader(reader), &expectedKind)
	if err != nil {
		return fmt.Errorf("create datafile reader for %s: %w", key, err)
	}

	var recordCount int64
	for i := int64(0); i < dataReader.Count(); i++ {
		record, err := dataReader.Read()
		if err != nil {
			return fmt.Errorf("read record %d from %s: %w", i, key, err)
		}
		if _, err := db.ReplicateRecord(record); err != nil {
			return fmt.Errorf("replicate record %d from %s: %w", i, key, err)
		}
		recordCount++
	}

	results, err := dataReader.Close()
	if err != nil {
		return fmt.Errorf("close datafile reader for %s: %w", key, err)
	}

	logger.Info("imported datafile",
		"key", key,
		"kind", results.Kind,
		"records", recordCount,
		"first_revision", results.FirstRevision,
		"last_revision", results.LastRevision,
	)

	return nil
}

// cleanupTempFiles removes any temporary files created during object-storage
// downloads for Netsy file imports.
func cleanupTempFiles(logger *slog.Logger, tempFiles []string) {
	for _, file := range tempFiles {
		if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
			logger.Warn("failed to remove temporary imported file", "file", file, "error", err)
		}
	}
}
