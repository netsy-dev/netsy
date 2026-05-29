// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"context"
	"fmt"
	"io"
	"testing"
)

func TestScopedStorage_PrefixesKeys(t *testing.T) {
	inner := NewMemoryStore()
	scoped := newScopedStorage(inner, "myprefix")
	ctx := context.Background()

	_ = scoped.Put(ctx, "chunks/file.netsy", []byte("data"))
	if _, _, err := inner.Get(ctx, "myprefix/chunks/file.netsy"); err != nil {
		t.Errorf("Put: expected data at prefixed key, got error: %v", err)
	}

	_ = scoped.PutIfMatch(ctx, "new/key", []byte("value"), "")
	if _, _, err := inner.Get(ctx, "myprefix/new/key"); err != nil {
		t.Errorf("PutIfMatch: expected data at prefixed key, got error: %v", err)
	}

	data, _, err := scoped.Get(ctx, "chunks/file.netsy")
	if err != nil {
		t.Errorf("Get: unexpected error: %v", err)
	}
	if string(data) != "data" {
		t.Errorf("Get: got %q, want %q", data, "data")
	}

	_ = scoped.Delete(ctx, "chunks/file.netsy")
	if _, _, err := inner.Get(ctx, "myprefix/chunks/file.netsy"); err != ErrNotFound {
		t.Errorf("Delete: expected key to be removed, got err=%v", err)
	}
}

func TestScopedStorage_ListPrefixesAndStrips(t *testing.T) {
	inner := NewMemoryStore()
	scoped := newScopedStorage(inner, "myprefix")
	ctx := context.Background()

	_ = inner.Put(ctx, "myprefix/chunks/0001.netsy", []byte("a"))
	_ = inner.Put(ctx, "myprefix/chunks/0002.netsy", []byte("bb"))
	_ = inner.Put(ctx, "other/chunks/0003.netsy", []byte("ccc"))

	results, err := scoped.List(ctx, "chunks/")
	if err != nil {
		t.Fatalf("List: unexpected error: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("List: got %d results, want 2", len(results))
	}

	for _, r := range results {
		if r.Key != "chunks/0001.netsy" && r.Key != "chunks/0002.netsy" {
			t.Errorf("List: unexpected key %q (prefix should be stripped)", r.Key)
		}
	}
}

func TestScopedStorage_TrailingSlash(t *testing.T) {
	inner := NewMemoryStore()
	ctx := context.Background()

	// Without trailing slash
	scoped1 := newScopedStorage(inner, "prefix")
	_ = scoped1.Put(ctx, "key", []byte("v1"))
	if _, _, err := inner.Get(ctx, "prefix/key"); err != nil {
		t.Errorf("without slash: expected data at prefix/key, got error: %v", err)
	}

	// With trailing slash — same result
	scoped2 := newScopedStorage(inner, "prefix/")
	data, _, err := scoped2.Get(ctx, "key")
	if err != nil {
		t.Errorf("with slash: unexpected error: %v", err)
	}
	if string(data) != "v1" {
		t.Errorf("with slash: got %q, want %q", data, "v1")
	}
}

func TestScopedStorage_ErrorPropagation(t *testing.T) {
	inner := &errorStorage{}
	scoped := newScopedStorage(inner, "prefix")

	_, err := scoped.List(context.Background(), "chunks/")
	if err == nil {
		t.Fatal("List: expected error, got nil")
	}
}

type errorStorage struct{}

func (e *errorStorage) Get(_ context.Context, _ string) ([]byte, string, error) {
	return nil, "", fmt.Errorf("get error")
}
func (e *errorStorage) Put(_ context.Context, _ string, _ []byte) error {
	return fmt.Errorf("put error")
}
func (e *errorStorage) PutIfMatch(_ context.Context, _ string, _ []byte, _ string) error {
	return fmt.Errorf("put error")
}
func (e *errorStorage) GetStream(_ context.Context, _ string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("get error")
}
func (e *errorStorage) PutStream(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return fmt.Errorf("put error")
}
func (e *errorStorage) Delete(_ context.Context, _ string) error {
	return fmt.Errorf("delete error")
}
func (e *errorStorage) List(_ context.Context, _ string) ([]ObjectInfo, error) {
	return nil, fmt.Errorf("list error")
}
