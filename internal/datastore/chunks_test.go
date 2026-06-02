// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/netsy-dev/netsy/internal/storage"
)

// mockStorage implements storage.ObjectStorage for testing
type mockStorage struct {
	objects []storage.ObjectInfo
	listErr error
}

func (m *mockStorage) Get(ctx context.Context, key string) ([]byte, string, error) {
	return nil, "", fmt.Errorf("not implemented")
}

func (m *mockStorage) Put(ctx context.Context, key string, data []byte) error {
	return nil
}

func (m *mockStorage) PutIfMatch(ctx context.Context, key string, data []byte, etag string) error {
	return nil
}

func (m *mockStorage) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockStorage) PutStream(ctx context.Context, key string, data io.Reader, size int64) error {
	return nil
}

func (m *mockStorage) PutStreamIfMatch(ctx context.Context, key string, data io.Reader, size int64, etag string) error {
	return nil
}

func (m *mockStorage) Delete(ctx context.Context, key string) error {
	return nil
}

func (m *mockStorage) List(ctx context.Context, prefix string) ([]storage.ObjectInfo, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	var results []storage.ObjectInfo
	for _, obj := range m.objects {
		if len(obj.Key) >= len(prefix) && obj.Key[:len(prefix)] == prefix {
			results = append(results, obj)
		}
	}
	return results, nil
}

func TestListChunks(t *testing.T) {
	store := &mockStorage{
		objects: []storage.ObjectInfo{
			{Key: "chunks/0001/0000000000000000001.netsy", Size: 100},
			{Key: "chunks/0002/0000000000000000002.netsy", Size: 200},
			{Key: "chunks/0003/0000000000000000003.netsy", Size: 300},
			{Key: "chunks/0005/0000000000000000005.netsy", Size: 500},
			{Key: "chunks/0004/notvalid.txt", Size: 50},
		},
	}

	chunks, err := ListChunks(context.Background(), store, 2)
	if err != nil {
		t.Fatalf("ListChunks: unexpected error: %v", err)
	}

	if len(chunks) != 2 {
		t.Fatalf("ListChunks: got %d chunks, want 2", len(chunks))
	}

	if chunks[0].Revision != 3 {
		t.Errorf("chunks[0].Revision = %d, want 3", chunks[0].Revision)
	}
	if chunks[1].Revision != 5 {
		t.Errorf("chunks[1].Revision = %d, want 5", chunks[1].Revision)
	}
}

func TestListChunks_Empty(t *testing.T) {
	store := &mockStorage{objects: []storage.ObjectInfo{}}

	chunks, err := ListChunks(context.Background(), store, 0)
	if err != nil {
		t.Fatalf("ListChunks: unexpected error: %v", err)
	}
	if len(chunks) != 0 {
		t.Fatalf("ListChunks: got %d chunks, want 0", len(chunks))
	}
}

func TestListChunks_ListError(t *testing.T) {
	store := &mockStorage{listErr: fmt.Errorf("connection refused")}

	_, err := ListChunks(context.Background(), store, 0)
	if err == nil {
		t.Fatal("ListChunks: expected error, got nil")
	}
}

func TestListChunksForCleanup(t *testing.T) {
	store := &mockStorage{
		objects: []storage.ObjectInfo{
			{Key: "chunks/0001/0000000000000000001.netsy", Size: 100},
			{Key: "chunks/0002/0000000000000000002.netsy", Size: 200},
			{Key: "chunks/0003/0000000000000000003.netsy", Size: 300},
			{Key: "chunks/0005/0000000000000000005.netsy", Size: 500},
		},
	}

	chunks, err := ListChunksForCleanup(context.Background(), store, 3)
	if err != nil {
		t.Fatalf("ListChunksForCleanup: unexpected error: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("ListChunksForCleanup: got %d chunks, want 3", len(chunks))
	}

	// Should be sorted oldest first
	if chunks[0].Revision != 1 || chunks[1].Revision != 2 || chunks[2].Revision != 3 {
		t.Errorf("ListChunksForCleanup: unexpected order: %v", chunks)
	}
}
