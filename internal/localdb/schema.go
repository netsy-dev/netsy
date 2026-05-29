// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"fmt"
	"log/slog"
)

// schemaVersion is the expected `PRAGMA user_version` value. Increment this
// whenever the schema changes. A mismatch at startup is fatal — a future
// release may migrate in-place or wipe and rebuild from object storage.
const schemaVersion = 1

var migrations = []string{
	`CREATE TABLE IF NOT EXISTS records (
		revision integer PRIMARY KEY NOT NULL,
		key blob NOT NULL,
		created integer NOT NULL,
		deleted integer NOT NULL,
		create_revision integer NOT NULL,
		prev_revision integer NOT NULL,
		version integer NOT NULL,
		lease integer NOT NULL,
		dek integer NOT NULL,
		value blob,
		created_at text NOT NULL,
		compacted_at text,
		leader_id text NOT NULL,
		replicated_at text
	);`,
	`CREATE UNIQUE INDEX IF NOT EXISTS records_key_create_rev_prev_rev_uindex ON records (key, create_revision, prev_revision)`,
	`CREATE INDEX IF NOT EXISTS records_index_key ON records (key);`,
	`CREATE TABLE IF NOT EXISTS compactions (
		compaction_revision integer PRIMARY KEY NOT NULL,
		created_at text NOT NULL
	);`,
}

// checkSchemaVersion reads PRAGMA user_version and enforces a strict match
// against the compiled-in schemaVersion. A fresh database (user_version == 0)
// is stamped with the current version. Any other value is a fatal mismatch.
func (db *database) checkSchemaVersion() error {
	var version int
	if err := db.conn.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("failed to read schema version: %w", err)
	}

	if version == 0 {
		if _, err := db.conn.Exec(fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
			return fmt.Errorf("failed to set schema version: %w", err)
		}
		return nil
	}

	if version != schemaVersion {
		return fmt.Errorf("schema version mismatch: database is version %d, expected %d", version, schemaVersion)
	}

	return nil
}

// applyMigrations executes each DDL statement in migrations.
func (db *database) applyMigrations() error {
	for _, stmt := range migrations {
		if _, err := db.conn.Exec(stmt); err != nil {
			slog.Error("error running migration", "migration", stmt, "error", err)
			return err
		}
	}
	return nil
}
