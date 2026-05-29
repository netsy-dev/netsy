// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"context"
	"fmt"

	"log/slog"

	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/metrics"
	pb "github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
)

// importLatestSnapshot imports the newest durable snapshot into an empty local
// database when object storage has one available.
func importLatestSnapshot(
	ctx context.Context,
	logger *slog.Logger,
	db localdb.Database,
	cfg *config.Config,
	snapshotInfo *datastore.LatestSnapshotInfo,
	storageClient storage.ObjectStorage,
	storageMetrics *metrics.ObjectStorageMetrics,
) error {
	if snapshotInfo == nil || !snapshotInfo.Found {
		return nil
	}

	logger.Info("database is empty, importing latest snapshot",
		"key", snapshotInfo.Key,
		"revision", snapshotInfo.Revision,
	)

	return datastore.DownloadAndImportFile(
		ctx,
		logger,
		db,
		storageClient,
		cfg.DataDir,
		snapshotInfo.Key,
		snapshotInfo.Size,
		pb.FileKind_KIND_SNAPSHOT,
		storageMetrics,
	)
}

// backfillChunksFromRevision replays every chunk file above fromRevision into
// the local database. Idempotent record replay allows this to overlap with
// live replication safely during bootstrap.
func backfillChunksFromRevision(
	ctx context.Context,
	logger *slog.Logger,
	db localdb.Database,
	cfg *config.Config,
	fromRevision int64,
	storageClient storage.ObjectStorage,
	storageMetrics *metrics.ObjectStorageMetrics,
) error {
	chunks, err := datastore.ListChunks(ctx, storageClient, fromRevision)
	if err != nil {
		return fmt.Errorf("list chunks: %w", err)
	}

	if len(chunks) == 0 {
		logger.Info("no chunk files found for backfill", "from_revision", fromRevision)
		return nil
	}

	logger.Info("backfilling chunk files",
		"from_revision", fromRevision,
		"count", len(chunks),
	)

	for _, chunk := range chunks {
		if err := datastore.DownloadAndImportFile(
			ctx,
			logger,
			db,
			storageClient,
			cfg.DataDir,
			chunk.Key,
			chunk.Size,
			pb.FileKind_KIND_CHUNK,
			storageMetrics,
		); err != nil {
			return fmt.Errorf("import chunk %s: %w", chunk.Key, err)
		}
	}

	return nil
}
