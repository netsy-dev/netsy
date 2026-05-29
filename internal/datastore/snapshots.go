// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"fmt"
	"sort"

	"github.com/netsy-dev/netsy/internal/storage"
)

// LatestSnapshotInfo contains information about the latest snapshot
type LatestSnapshotInfo struct {
	Revision int64
	Key      string
	Size     int64
	Found    bool
}

// SnapshotKey generates the object storage key for a snapshot file
func SnapshotKey(revision int64) string {
	return fmt.Sprintf("snapshots/%019d.netsy", revision)
}

// ListSnapshots returns all snapshot files sorted by revision (newest first)
func ListSnapshots(ctx context.Context, store storage.ObjectStorage) ([]FileInfo, error) {
	objects, err := store.List(ctx, "snapshots/")
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshot objects: %w", err)
	}

	var snapshots []FileInfo
	for _, obj := range objects {
		rev, ok := parseRevisionFromKey(obj.Key)
		if !ok {
			continue
		}
		snapshots = append(snapshots, FileInfo{Key: obj.Key, Size: obj.Size, Revision: rev})
	}

	sort.Slice(snapshots, func(i, j int) bool {
		return snapshots[i].Revision > snapshots[j].Revision
	})

	return snapshots, nil
}

// GetLatestSnapshot returns information about the latest snapshot, or Found=false if none exists
func GetLatestSnapshot(ctx context.Context, store storage.ObjectStorage) (*LatestSnapshotInfo, error) {
	snapshots, err := ListSnapshots(ctx, store)
	if err != nil {
		return nil, err
	}

	if len(snapshots) == 0 {
		return &LatestSnapshotInfo{Found: false}, nil
	}

	latest := snapshots[0]
	return &LatestSnapshotInfo{
		Revision: latest.Revision,
		Key:      latest.Key,
		Size:     latest.Size,
		Found:    true,
	}, nil
}
