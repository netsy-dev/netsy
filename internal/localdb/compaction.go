// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"database/sql"
	"fmt"
	"time"
)

// LatestCompactionRevision returns the highest persisted compaction revision,
// or 0 when no compaction revision has been recorded yet.
func (db *database) LatestCompactionRevision() (int64, error) {
	const query = "SELECT compaction_revision FROM compactions ORDER BY compaction_revision DESC LIMIT 1"

	var revision int64
	err := db.conn.QueryRow(query).Scan(&revision)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}

	return revision, nil
}

// DeriveCompactionRevision infers the compaction revision from the
// contiguous prefix of records whose compacted_at field is already set.
func (db *database) DeriveCompactionRevision() (int64, error) {
	const firstUncompacted = "SELECT revision FROM records WHERE compacted_at IS NULL ORDER BY revision ASC LIMIT 1"

	var firstGap int64
	err := db.conn.QueryRow(firstUncompacted).Scan(&firstGap)
	switch err {
	case nil:
		if firstGap <= 1 {
			return 0, nil
		}
		return firstGap - 1, nil
	case sql.ErrNoRows:
		return db.LatestRevision()
	default:
		return 0, err
	}
}

// PersistCompactionRevision records a compaction revision if it is
// non-zero and not already present.
func (db *database) PersistCompactionRevision(revision int64) error {
	if revision <= 0 {
		return nil
	}

	const query = `INSERT OR IGNORE INTO compactions (
		compaction_revision,
		created_at
	) VALUES (?, ?)`

	createdAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.conn.Exec(query, revision, createdAt); err != nil {
		return fmt.Errorf("persist compaction revision %d: %w", revision, err)
	}

	return nil
}

// ExecuteCompaction sets value to NULL and compacted_at to the current
// timestamp for historical records at or below the given Compaction Revision
// that have not already been compacted. This matches etcd semantics where the
// compaction revision is inclusive: old revisions at or below the compaction
// point become unavailable, while each key's newest revision remains readable
// for current-revision range requests. Returns the number of affected rows.
func (db *database) ExecuteCompaction(compactionRevision int64) (int64, error) {
	if compactionRevision <= 0 {
		return 0, nil
	}

	// Compact records at or below the compaction revision when they are either a
	// tombstone or have been superseded by any newer revision for the same key.
	// Non-deleted records with no newer same-key revision are preserved so current
	// requests can still read them after historical revisions are compacted.
	const query = `UPDATE records AS r
		SET value = NULL, compacted_at = ?
		WHERE r.revision <= ?
			AND r.compacted_at IS NULL
			AND (
				r.deleted = 1
				OR EXISTS (
					SELECT 1
					FROM records AS newer
					WHERE newer.key = r.key
						AND newer.revision > r.revision
				)
			)`

	compactedAt := time.Now().UTC().Format(time.RFC3339Nano)
	result, err := db.conn.Exec(query, compactedAt, compactionRevision)
	if err != nil {
		return 0, fmt.Errorf("execute compaction below revision %d: %w", compactionRevision, err)
	}

	return result.RowsAffected()
}
