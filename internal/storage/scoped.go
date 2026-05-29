// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"io"
	"strings"
)

// scopedStorage wraps an ObjectStorage and prepends a prefix to all keys
type scopedStorage struct {
	underlying ObjectStorage
	prefix     string
}

func newScopedStorage(underlying ObjectStorage, prefix string) ObjectStorage {
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	return &scopedStorage{underlying: underlying, prefix: prefix}
}

func (s *scopedStorage) Get(ctx context.Context, key string) ([]byte, string, error) {
	return s.underlying.Get(ctx, s.prefix+key)
}

func (s *scopedStorage) Put(ctx context.Context, key string, data []byte) error {
	return s.underlying.Put(ctx, s.prefix+key, data)
}

func (s *scopedStorage) PutIfMatch(ctx context.Context, key string, data []byte, etag string) error {
	return s.underlying.PutIfMatch(ctx, s.prefix+key, data, etag)
}

func (s *scopedStorage) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	return s.underlying.GetStream(ctx, s.prefix+key)
}

func (s *scopedStorage) PutStream(ctx context.Context, key string, r io.Reader, size int64) error {
	return s.underlying.PutStream(ctx, s.prefix+key, r, size)
}

func (s *scopedStorage) Delete(ctx context.Context, key string) error {
	return s.underlying.Delete(ctx, s.prefix+key)
}

func (s *scopedStorage) List(ctx context.Context, prefix string) ([]ObjectInfo, error) {
	results, err := s.underlying.List(ctx, s.prefix+prefix)
	if err != nil {
		return nil, err
	}
	// Strip the scope prefix from returned keys
	for i := range results {
		results[i].Key = strings.TrimPrefix(results[i].Key, s.prefix)
	}
	return results, nil
}
