// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"fmt"
	"log/slog"

	"github.com/netsy-dev/netsy/internal/config"
)

// New creates an ObjectStorage provider based on config.
// Returns the provider, a cleanup function, and any error.
func New(cfg *config.Config, logger *slog.Logger) (ObjectStorage, func(), error) {
	var store ObjectStorage
	var cleanup func()

	switch cfg.Storage.Provider {
	case "s3":
		p, err := newS3Provider(cfg, logger)
		if err != nil {
			return nil, nil, err
		}
		store = p
		cleanup = func() {}
	case "gcs":
		p, err := newGCSProvider(cfg, logger)
		if err != nil {
			return nil, nil, err
		}
		store = p
		cleanup = func() { p.client.Close() }
	default:
		return nil, nil, fmt.Errorf("unsupported storage provider: %q", cfg.Storage.Provider)
	}

	// Apply key prefix scoping if configured
	if cfg.Storage.KeyPrefix != "" {
		store = newScopedStorage(store, cfg.Storage.KeyPrefix)
	}

	return store, cleanup, nil
}
