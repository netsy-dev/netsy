// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"context"
	"fmt"
	"os"

	"github.com/netsy-dev/netsy/internal/storage"
)

// Upload uploads a local file to object storage at the given key
func Upload(ctx context.Context, store storage.ObjectStorage, key, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file %s: %w", filePath, err)
	}

	if err := store.PutStream(ctx, key, file, fileInfo.Size()); err != nil {
		return fmt.Errorf("failed to upload %s: %w", key, err)
	}

	return nil
}
