// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/netsy-dev/netsy/internal/storage"
)

const membersFilePath = "members.json"

// MembersFile is the JSON structure stored at members.json. The ETag field
// tracks the object storage version for conditional writes and is not
// serialised to JSON.
type MembersFile struct {
	ClusterID string            `json:"cluster_id"`
	Members   map[string]uint64 `json:"members"` // node_id -> member_id
	ETag      string            `json:"-"`
}

// ReadMembersFile reads and unmarshals the members file from object storage.
// The returned MembersFile carries the ETag for subsequent conditional writes.
// Returns storage.ErrNotFound if the file does not exist.
func ReadMembersFile(ctx context.Context, store storage.ObjectStorage) (MembersFile, error) {
	data, etag, err := store.Get(ctx, membersFilePath)
	if err != nil {
		return MembersFile{}, err
	}

	var mf MembersFile
	if err := json.Unmarshal(data, &mf); err != nil {
		return MembersFile{}, fmt.Errorf("discovery: unmarshal members file: %w", err)
	}
	mf.ETag = etag
	return mf, nil
}

// WriteMembersFile writes the members file to object storage using PutIfMatch
// for conditional write semantics. An empty ETag on the MembersFile means the
// file must not exist (create-only).
func WriteMembersFile(ctx context.Context, store storage.ObjectStorage, mf MembersFile) error {
	data, err := json.Marshal(mf)
	if err != nil {
		return fmt.Errorf("discovery: marshal members file: %w", err)
	}
	if err := store.PutIfMatch(ctx, membersFilePath, data, mf.ETag); err != nil {
		return fmt.Errorf("discovery: write members file: %w", err)
	}
	return nil
}

// AllocateMemberID returns the next member ID by incrementing the highest
// existing member ID. The first allocated ID is 1.
func AllocateMemberID(existing MembersFile) uint64 {
	var max uint64
	for _, id := range existing.Members {
		if id > max {
			max = id
		}
	}
	return max + 1
}

// FindMemberID returns the member_id for a given node_id if it exists
// in the members file.
func FindMemberID(mf MembersFile, nodeID string) (uint64, bool) {
	id, ok := mf.Members[nodeID]
	return id, ok
}
