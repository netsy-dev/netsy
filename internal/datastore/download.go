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
	"path/filepath"
	"time"

	"github.com/netsy-dev/netsy/internal/datafile"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/metrics"
	pb "github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
)

// downloadToTempFile streams a file into a temporary file and returns a file
// handle positioned at the start of the file.
func downloadToTempFile(ctx context.Context, store storage.ObjectStorage, key string, tempDir string) (*os.File, int64, error) {
	reader, err := store.GetStream(ctx, key)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to download file: %w", err)
	}
	defer reader.Close()

	if tempDir != "" {
		tempDir = filepath.Join(tempDir, "tmp")
		if err := os.MkdirAll(tempDir, 0o700); err != nil {
			return nil, 0, fmt.Errorf("failed to create temporary directory: %w", err)
		}
	}

	tempFile, err := os.CreateTemp(tempDir, "netsy_*.netsy")
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create temporary file: %w", err)
	}
	removeTemp := true
	// Clean up the temp file only if copy or seek fails. On success, ownership of
	// the open file handle passes to the caller who is responsible for closing it.
	defer func() {
		if removeTemp {
			path := tempFile.Name()
			tempFile.Close()
			os.Remove(path)
		}
	}()

	size, err := io.Copy(tempFile, reader)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to write to temporary file: %w", err)
	}
	if _, err := tempFile.Seek(0, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("failed to seek temporary file: %w", err)
	}

	removeTemp = false
	return tempFile, size, nil
}

// DownloadAndImportFile downloads a Netsy chunk or snapshot file from object
// storage and replays its records into SQLite.
func DownloadAndImportFile(
	ctx context.Context,
	logger *slog.Logger,
	db localdb.Database,
	store storage.ObjectStorage,
	key string,
	expectedKind pb.FileKind,
	storageMetrics *metrics.ObjectStorageMetrics,
) error {
	readStart := time.Now()
	reader, err := store.GetStream(ctx, key)
	if storageMetrics != nil {
		storageMetrics.ObserveRead(time.Since(readStart), err)
	}
	if err != nil {
		return fmt.Errorf("download %s: failed to download file: %w", key, err)
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
