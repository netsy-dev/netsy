// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/netsy-dev/netsy/internal/datafile"
	pb "github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
)

// BootstrapSnapshotKey is the fixed object storage key for an operator-provided
// snapshot used only when creating a brand new cluster.
const BootstrapSnapshotKey = "bootstrap.netsy"

// bootstrapInitialRecordKey is Netsy's internal key that must occupy revision 1
// in every valid cluster history.
const bootstrapInitialRecordKey = "_netsy"

// PromoteBootstrapSnapshot validates bootstrap.netsy and creates the equivalent
// normal durable snapshot before cluster initialisation is committed. If the
// bootstrap snapshot is absent, it returns Found=false. If promotion was already
// completed by a previous attempt, the write is idempotent when the existing
// snapshot has identical contents.
func PromoteBootstrapSnapshot(ctx context.Context, store storage.ObjectStorage, tempDir string) (*LatestSnapshotInfo, error) {
	bootstrapFile, size, err := downloadToTempFile(ctx, store, BootstrapSnapshotKey, tempDir)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &LatestSnapshotInfo{Found: false}, nil
		}
		return nil, fmt.Errorf("read bootstrap snapshot: %w", err)
	}
	defer func() {
		path := bootstrapFile.Name()
		bootstrapFile.Close()
		os.Remove(path)
	}()

	lastRevision, err := validateBootstrapSnapshot(bootstrapFile)
	if err != nil {
		return nil, err
	}
	targetKey := SnapshotKey(lastRevision)

	chunks, err := store.List(ctx, "chunks/")
	if err != nil {
		return nil, fmt.Errorf("list chunks before bootstrap snapshot promotion: %w", err)
	}
	if len(chunks) > 0 {
		return nil, fmt.Errorf("bootstrap snapshot %s cannot be promoted when chunks already exist", BootstrapSnapshotKey)
	}

	snapshots, err := ListSnapshots(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("list snapshots before bootstrap snapshot promotion: %w", err)
	}
	for _, snapshot := range snapshots {
		if snapshot.Key != targetKey {
			return nil, fmt.Errorf("bootstrap snapshot %s cannot be promoted when normal snapshot %s already exists", BootstrapSnapshotKey, snapshot.Key)
		}
	}

	if err := storage.PutStreamIfAbsent(ctx, store, targetKey, bootstrapFile, size); err != nil {
		return nil, fmt.Errorf("promote bootstrap snapshot to %s: %w", targetKey, err)
	}

	return &LatestSnapshotInfo{
		Revision: lastRevision,
		Key:      targetKey,
		Size:     size,
		Found:    true,
	}, nil
}

// validateBootstrapSnapshot verifies that bootstrap.netsy is a valid Netsy snapshot
// file suitable for seeding a new cluster and returns its last revision. The
// datafile reader performs the generic file validation: expected kind, header CRC,
// record CRCs, footer CRC, and aggregate records CRC. Bootstrap promotion adds
// the stricter seed-state requirements that the snapshot is non-empty, contains
// contiguous revisions starting at 1, and starts with Netsy's internal initial
// record.
func validateBootstrapSnapshot(data io.Reader) (int64, error) {
	kind := pb.FileKind_KIND_SNAPSHOT
	reader, err := datafile.NewReader(bufio.NewReader(data), &kind)
	if err != nil {
		return 0, fmt.Errorf("validate bootstrap snapshot: %w", err)
	}
	if reader.Count() == 0 {
		return 0, fmt.Errorf("validate bootstrap snapshot: snapshot is empty")
	}

	var previousRevision int64
	for i := int64(0); i < reader.Count(); i++ {
		record, err := reader.Read()
		if err != nil {
			return 0, fmt.Errorf("validate bootstrap snapshot: read record %d: %w", i, err)
		}
		if record.GetRevision() != previousRevision+1 {
			return 0, fmt.Errorf("validate bootstrap snapshot: record %d has revision %d after %d", i, record.GetRevision(), previousRevision)
		}
		if i == 0 && string(record.GetKey()) != bootstrapInitialRecordKey {
			return 0, fmt.Errorf("validate bootstrap snapshot: record %d has key %q, want %q", i, record.GetKey(), bootstrapInitialRecordKey)
		}
		previousRevision = record.GetRevision()
	}

	results, err := reader.Close()
	if err != nil {
		return 0, fmt.Errorf("validate bootstrap snapshot: %w", err)
	}
	if results.LastRevision != reader.Count() {
		return 0, fmt.Errorf("validate bootstrap snapshot: records count %d does not match last revision %d", reader.Count(), results.LastRevision)
	}

	return results.LastRevision, nil
}
