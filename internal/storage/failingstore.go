// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"fmt"
	"io"
	"sync"
)

// FailingStore wraps an ObjectStorage implementation and can be toggled to
// fail specific operations on demand. It is intended for use in integration
// tests that exercise error handling and recovery paths.
type FailingStore struct {
	mu      sync.RWMutex
	inner   ObjectStorage
	failPut bool
	failGet bool
}

// NewFailingStore returns a FailingStore that delegates to inner.
func NewFailingStore(inner ObjectStorage) *FailingStore {
	return &FailingStore{inner: inner}
}

// SetFailPut toggles whether Put, PutIfMatch, PutStream, and PutStreamIfMatch calls fail.
func (f *FailingStore) SetFailPut(fail bool) {
	f.mu.Lock()
	f.failPut = fail
	f.mu.Unlock()
}

// SetFailGet toggles whether Get and GetStream calls fail.
func (f *FailingStore) SetFailGet(fail bool) {
	f.mu.Lock()
	f.failGet = fail
	f.mu.Unlock()
}

func (f *FailingStore) shouldFailPut() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.failPut
}

func (f *FailingStore) shouldFailGet() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.failGet
}

// Get delegates to the inner store, or returns an error if get failures are enabled.
func (f *FailingStore) Get(ctx context.Context, key string) ([]byte, string, error) {
	if f.shouldFailGet() {
		return nil, "", fmt.Errorf("failingstore: simulated get failure")
	}
	return f.inner.Get(ctx, key)
}

// Put delegates to the inner store, or returns an error if put failures are enabled.
func (f *FailingStore) Put(ctx context.Context, key string, data []byte) error {
	if f.shouldFailPut() {
		return fmt.Errorf("failingstore: simulated put failure")
	}
	return f.inner.Put(ctx, key, data)
}

// PutIfMatch delegates to the inner store, or returns an error if put failures are enabled.
func (f *FailingStore) PutIfMatch(ctx context.Context, key string, data []byte, etag string) error {
	if f.shouldFailPut() {
		return fmt.Errorf("failingstore: simulated put failure")
	}
	return f.inner.PutIfMatch(ctx, key, data, etag)
}

// GetStream delegates to the inner store, or returns an error if get failures are enabled.
func (f *FailingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	if f.shouldFailGet() {
		return nil, fmt.Errorf("failingstore: simulated get failure")
	}
	return f.inner.GetStream(ctx, key)
}

// PutStream delegates to the inner store, or returns an error if put failures are enabled.
func (f *FailingStore) PutStream(ctx context.Context, key string, r io.Reader, size int64) error {
	if f.shouldFailPut() {
		return fmt.Errorf("failingstore: simulated put failure")
	}
	return f.inner.PutStream(ctx, key, r, size)
}

// PutStreamIfMatch delegates to the inner store, or returns an error if put failures are enabled.
func (f *FailingStore) PutStreamIfMatch(ctx context.Context, key string, r io.Reader, size int64, etag string) error {
	if f.shouldFailPut() {
		return fmt.Errorf("failingstore: simulated put failure")
	}
	return f.inner.PutStreamIfMatch(ctx, key, r, size, etag)
}

// Delete delegates to the inner store.
func (f *FailingStore) Delete(ctx context.Context, key string) error {
	return f.inner.Delete(ctx, key)
}

// List delegates to the inner store.
func (f *FailingStore) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	return f.inner.List(ctx, prefix)
}
