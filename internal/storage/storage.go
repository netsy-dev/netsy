// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"github.com/podplane/s3lect"
)

// Common storage errors — use s3lect's errors for compatibility.
var (
	ErrNotFound     = s3lect.ErrStorageNotFound
	ErrPrecondition = s3lect.ErrStoragePrecondition
)

// ObjectInfo represents metadata about an object in storage.
type ObjectInfo struct {
	Key  string
	Size int64
}

// ObjectStorage defines the interface for object storage operations.
// Buffered methods (Get, Put, PutIfMatch) operate on whole objects in memory
// and are intended for small blobs/files. Streaming methods
// (GetStream, PutStream, PutStreamIfMatch) are intended for large data files.
type ObjectStorage interface {
	// Get retrieves an object and returns its contents and ETag.
	// Returns ErrNotFound when the key does not exist.
	Get(ctx context.Context, key string) ([]byte, string, error)

	// Put stores an object in storage.
	Put(ctx context.Context, key string, data []byte) error

	// PutIfMatch stores an object only if the ETag matches.
	// An empty etag means the object must not exist (create-only).
	// Returns ErrPrecondition when the precondition is not met.
	PutIfMatch(ctx context.Context, key string, data []byte, etag string) error

	// GetStream retrieves an object as a stream.
	// Returns ErrNotFound when the key does not exist.
	GetStream(ctx context.Context, key string) (io.ReadCloser, error)

	// PutStream stores an object from a stream.
	PutStream(ctx context.Context, key string, r io.Reader, size int64) error

	// PutStreamIfMatch stores an object from a stream only if the ETag matches.
	// An empty etag means the object must not exist (create-only).
	// Returns ErrPrecondition when the precondition is not met.
	PutStreamIfMatch(ctx context.Context, key string, r io.Reader, size int64, etag string) error

	// Delete removes the object at the given key.
	Delete(ctx context.Context, key string) error

	// List returns all object keys matching the given prefix.
	List(ctx context.Context, prefix string) ([]ObjectInfo, error)
}

// PutIfAbsent creates an object only if it does not already exist. If the
// object already exists with identical contents, it returns nil. If it exists
// with different contents, it returns an error.
func PutIfAbsent(ctx context.Context, store ObjectStorage, key string, data []byte) error {
	err := store.PutIfMatch(ctx, key, data, "")
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrPrecondition) {
		return fmt.Errorf("object storage upload %s: %w", key, err)
	}

	existing, _, getErr := store.Get(ctx, key)
	if getErr != nil {
		return fmt.Errorf("read existing object %s after already-exists: %w", key, getErr)
	}
	if !bytes.Equal(existing, data) {
		return fmt.Errorf("object %s already exists with different contents", key)
	}
	return nil
}

// PutStreamIfAbsent creates an object from a stream only if it does not already
// exist. If the object already exists with identical contents, it returns nil.
// If it exists with different contents, it returns an error.
func PutStreamIfAbsent(ctx context.Context, store ObjectStorage, key string, data io.ReadSeeker, size int64) error {
	if _, err := data.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek object data before upload %s: %w", key, err)
	}
	// First try the normal create-only write.
	err := store.PutStreamIfMatch(ctx, key, data, size, "")
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrPrecondition) {
		return fmt.Errorf("object storage upload %s: %w", key, err)
	}

	// The object already exists. Treat that as idempotent only if the existing
	// contents match the data we attempted to write.
	existing, getErr := store.GetStream(ctx, key)
	if getErr != nil {
		return fmt.Errorf("read existing object %s after already-exists: %w", key, getErr)
	}
	defer existing.Close()

	if _, err := data.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek object data before compare %s: %w", key, err)
	}
	// Hash both streams instead of buffering whole files in memory.
	existingHash := sha256.New()
	if _, err := io.Copy(existingHash, existing); err != nil {
		return fmt.Errorf("hash existing object %s after already-exists: %w", key, err)
	}
	dataHash := sha256.New()
	if _, err := io.Copy(dataHash, data); err != nil {
		return fmt.Errorf("hash object data %s after already-exists: %w", key, err)
	}
	if !bytes.Equal(existingHash.Sum(nil), dataHash.Sum(nil)) {
		return fmt.Errorf("object %s already exists with different contents", key)
	}
	return nil
}
