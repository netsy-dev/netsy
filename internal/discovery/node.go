// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/netsy-dev/netsy/internal/storage"
)

// NodeRegistration is the JSON structure stored at nodes/{node_id}.json.
type NodeRegistration struct {
	NodeID                 string `json:"node_id"`
	ClientAdvertiseAddress string `json:"client_advertise_address"`
	PeerAdvertiseAddress   string `json:"peer_advertise_address"`
}

// NodePath returns the object storage path for a node registration file.
func NodePath(nodeID string) string {
	return "nodes/" + nodeID + ".json"
}

// WriteNodeRegistration writes a node registration file to object storage.
// If an equivalent file already exists, the write is a no-op. If a file
// exists with different values, an error is returned to prevent silent
// overwrites.
func WriteNodeRegistration(ctx context.Context, store storage.ObjectStorage, reg NodeRegistration) error {
	path := NodePath(reg.NodeID)

	existing, _, err := store.Get(ctx, path)
	if err == nil {
		var prev NodeRegistration
		if uerr := json.Unmarshal(existing, &prev); uerr != nil {
			return fmt.Errorf("discovery: unmarshal existing node registration %s: %w", path, uerr)
		}
		if prev == reg {
			return nil
		}
		return fmt.Errorf("discovery: node registration %s already exists with different values", reg.NodeID)
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("discovery: read existing node registration %s: %w", path, err)
	}

	data, err := json.Marshal(reg)
	if err != nil {
		return fmt.Errorf("discovery: marshal node registration: %w", err)
	}
	if err := store.Put(ctx, path, data); err != nil {
		return fmt.Errorf("discovery: write node registration %s: %w", path, err)
	}
	return nil
}

// ReadNodeRegistration reads and unmarshals a node registration file from
// object storage. Returns storage.ErrNotFound if the file does not exist.
func ReadNodeRegistration(ctx context.Context, store storage.ObjectStorage, nodeID string) (NodeRegistration, error) {
	path := NodePath(nodeID)

	data, _, err := store.Get(ctx, path)
	if err != nil {
		return NodeRegistration{}, err
	}

	var reg NodeRegistration
	if err := json.Unmarshal(data, &reg); err != nil {
		return NodeRegistration{}, fmt.Errorf("discovery: unmarshal node registration %s: %w", path, err)
	}
	return reg, nil
}

// DeleteNodeRegistration deletes a node registration file from object storage.
func DeleteNodeRegistration(ctx context.Context, store storage.ObjectStorage, nodeID string) error {
	if err := store.Delete(ctx, NodePath(nodeID)); err != nil {
		return fmt.Errorf("discovery: delete node registration %s: %w", nodeID, err)
	}
	return nil
}

// ListNodeRegistrations lists all node registration files under the nodes/
// prefix and reads each one. Files that fail to parse are silently skipped.
func ListNodeRegistrations(ctx context.Context, store storage.ObjectStorage) ([]NodeRegistration, error) {
	objects, err := store.List(ctx, "nodes/")
	if err != nil {
		return nil, fmt.Errorf("discovery: list node registrations: %w", err)
	}

	var registrations []NodeRegistration
	for _, obj := range objects {
		data, _, err := store.Get(ctx, obj.Key)
		if err != nil {
			slog.Warn("discovery: skipping unreadable node registration", "key", obj.Key, "error", err)
			continue
		}
		var reg NodeRegistration
		if err := json.Unmarshal(data, &reg); err != nil {
			slog.Warn("discovery: skipping unparseable node registration", "key", obj.Key, "error", err)
			continue
		}
		registrations = append(registrations, reg)
	}
	return registrations, nil
}
