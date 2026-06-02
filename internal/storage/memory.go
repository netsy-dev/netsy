// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"io"
	"strings"
	"sync"
)

// MemoryStore is an in-memory ObjectStorage for use in tests. It is safe
// for concurrent use. ETags are computed as MD5 hex digests of the stored
// content, matching S3 ETag semantics.
type MemoryStore struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: make(map[string][]byte)}
}

// contentETag returns the MD5 hex digest of data, matching S3 ETag format.
func contentETag(data []byte) string {
	h := md5.Sum(data)
	return hex.EncodeToString(h[:])
}

func (m *MemoryStore) Get(_ context.Context, key string) ([]byte, string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[key]
	if !ok {
		return nil, "", ErrNotFound
	}
	return data, contentETag(data), nil
}

func (m *MemoryStore) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.objects[key] = data
	return nil
}

func (m *MemoryStore) PutIfMatch(_ context.Context, key string, data []byte, etag string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.objects[key]
	if etag == "" && exists {
		return ErrPrecondition
	}
	if etag != "" && !exists {
		return ErrPrecondition
	}
	if etag != "" && exists && etag != contentETag(existing) {
		return ErrPrecondition
	}
	m.objects[key] = data
	return nil
}

func (m *MemoryStore) GetStream(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MemoryStore) PutStream(_ context.Context, key string, r io.Reader, _ int64) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.objects[key] = data
	return nil
}

func (m *MemoryStore) PutStreamIfMatch(_ context.Context, key string, r io.Reader, _ int64, etag string) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	existing, exists := m.objects[key]
	if etag == "" && exists {
		return ErrPrecondition
	}
	if etag != "" && !exists {
		return ErrPrecondition
	}
	if etag != "" && exists && etag != contentETag(existing) {
		return ErrPrecondition
	}
	m.objects[key] = data
	return nil
}

func (m *MemoryStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.objects, key)
	return nil
}

func (m *MemoryStore) List(_ context.Context, prefix string) ([]ObjectInfo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var results []ObjectInfo
	for key, data := range m.objects {
		if strings.HasPrefix(key, prefix) {
			results = append(results, ObjectInfo{Key: key, Size: int64(len(data))})
		}
	}
	return results, nil
}
