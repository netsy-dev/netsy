// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/netsy-dev/netsy/internal/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ErrReplicateConflict is returned when a replicated record has the same
// revision as an existing record but different contents.
var ErrReplicateConflict = errors.New("replicate conflict")

// ReplicateRecord copies an authoritative record into SQLite during live
// replication or object-storage backfill. It preserves the incoming revision
// and compaction metadata, stamps replicated_at locally, and treats exact
// duplicate copies as idempotent. It does not validate record semantics or
// allocate revisions, so callers must only use it with records from an
// authoritative source.
func (db *database) ReplicateRecord(record *proto.Record) (*proto.Record, error) {
	// do not allow zero values for revision
	if record.Revision == 0 {
		return nil, fmt.Errorf("cannot insert record with revision=0")
	}

	// set replicated at
	record.ReplicatedAt = timestamppb.Now()

	// prepare data
	query := `INSERT INTO records (` +
		`revision, ` +
		`key, ` +
		`created, ` +
		`deleted, ` +
		`create_revision, ` +
		`prev_revision, ` +
		`version, ` +
		`lease, ` +
		`dek, ` +
		`value, ` +
		`created_at, ` +
		`compacted_at, ` +
		`leader_id, ` +
		`replicated_at ` +
		`) VALUES (` +
		`?1, ` + // revision
		`?2, ` + // key
		`?3, ` + // created
		`?4, ` + // deleted
		`?5, ` + // create_revision
		`?6, ` + // prev_revision
		`?7, ` + // version
		`?8, ` + // lease
		`?9, ` + // dek
		`?10, ` + // value
		`?11, ` + // created_at
		`?12, ` + // compacted_at
		`?13, ` + // leader_id
		`?14 ` + // replicated_at
		`) RETURNING *`

	// insert record
	var createdAtStr string
	var compactedAtStr interface{}
	var replicatedAtStr interface{}
	if record.CreatedAt != nil {
		createdAtStr = record.CreatedAt.AsTime().Format(time.RFC3339Nano)
	}
	if record.CompactedAt != nil {
		compactedAtStr = record.CompactedAt.AsTime().Format(time.RFC3339Nano)
	}
	if record.ReplicatedAt != nil {
		replicatedAtStr = record.ReplicatedAt.AsTime().Format(time.RFC3339Nano)
	}

	// insert record and get returned values
	var returnedRecord proto.Record
	var returnedCreatedAtStr string
	var returnedCompactedAtStr, returnedReplicatedAtStr sql.NullString
	err := db.conn.QueryRow(
		query,
		record.Revision,       // 1
		record.Key,            // 2
		record.Created,        // 3
		record.Deleted,        // 4
		record.CreateRevision, // 5
		record.PrevRevision,   // 6
		record.Version,        // 7
		record.Lease,          // 8
		record.Dek,            // 9
		record.Value,          // 10
		createdAtStr,          // 11
		compactedAtStr,        // 12
		record.LeaderId,       // 13
		replicatedAtStr,       // 14
	).Scan(
		&returnedRecord.Revision,
		&returnedRecord.Key,
		&returnedRecord.Created,
		&returnedRecord.Deleted,
		&returnedRecord.CreateRevision,
		&returnedRecord.PrevRevision,
		&returnedRecord.Version,
		&returnedRecord.Lease,
		&returnedRecord.Dek,
		&returnedRecord.Value,
		&returnedCreatedAtStr,
		&returnedCompactedAtStr,
		&returnedRecord.LeaderId,
		&returnedReplicatedAtStr,
	)
	if err != nil {
		// Duplicate inserts are acceptable during replication/backfill if the
		// existing row is semantically identical to the incoming record.
		if strings.Contains(err.Error(), "UNIQUE constraint failed") || strings.Contains(err.Error(), "PRIMARY KEY constraint failed") {
			existing, findErr := db.FindRecordByRev(record.GetRevision())
			if findErr != nil {
				return nil, fmt.Errorf("read existing revision %d after duplicate insert: %w", record.GetRevision(), findErr)
			}
			if !recordsEqualForReplication(existing, record) {
				return nil, fmt.Errorf("%w: revision %d already exists with different contents", ErrReplicateConflict, record.GetRevision())
			}
			return existing, nil
		}
		return nil, err
	}

	// check insert ID matches revision
	if returnedRecord.Revision != record.Revision {
		return nil, fmt.Errorf("Unexpected error: insert ID (%d) does not match revision (%d)", returnedRecord.Revision, record.Revision)
	}

	// Convert string timestamps back to protobuf timestamps
	if returnedCreatedAtStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, returnedCreatedAtStr); err == nil {
			returnedRecord.CreatedAt = timestamppb.New(t)
		}
	}
	if returnedCompactedAtStr.Valid && returnedCompactedAtStr.String != "" {
		if t, err := time.Parse(time.RFC3339Nano, returnedCompactedAtStr.String); err == nil {
			returnedRecord.CompactedAt = timestamppb.New(t)
		}
	}
	if returnedReplicatedAtStr.Valid && returnedReplicatedAtStr.String != "" {
		if t, err := time.Parse(time.RFC3339Nano, returnedReplicatedAtStr.String); err == nil {
			returnedRecord.ReplicatedAt = timestamppb.New(t)
		}
	}

	return &returnedRecord, nil
}

// recordsEqualForReplication compares the replication-significant fields of
// two records when deciding whether a duplicate copy is idempotent.
func recordsEqualForReplication(a, b *proto.Record) bool {
	if a == nil || b == nil {
		return a == b
	}

	return a.GetRevision() == b.GetRevision() &&
		bytes.Equal(a.GetKey(), b.GetKey()) &&
		a.GetCreated() == b.GetCreated() &&
		a.GetDeleted() == b.GetDeleted() &&
		a.GetCreateRevision() == b.GetCreateRevision() &&
		a.GetPrevRevision() == b.GetPrevRevision() &&
		a.GetVersion() == b.GetVersion() &&
		a.GetLease() == b.GetLease() &&
		a.GetDek() == b.GetDek() &&
		bytes.Equal(a.GetValue(), b.GetValue()) &&
		a.GetLeaderId() == b.GetLeaderId() &&
		sameTimestamp(a.GetCreatedAt(), b.GetCreatedAt()) &&
		sameTimestamp(a.GetCompactedAt(), b.GetCompactedAt())
}

// sameTimestamp compares protobuf timestamps while preserving nil semantics.
func sameTimestamp(a, b *timestamppb.Timestamp) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.AsTime().Equal(b.AsTime())
}

// ReplicateTentativeRecord copies an authoritative record into SQLite,
// allowing overwrite of tentative records whose revision exceeds
// committedRevision. Records at or below committedRevision are treated
// as committed and handled by the standard ReplicateRecord path, which
// only accepts exact duplicates.
//
// This method supports the same-revision retry scenario: after a quorum
// rollback, the Primary retries via Path 1 (sync object storage) and
// reuses the same revision number. Replicas that stored the tentative
// record accept the overwrite because records above committed_revision
// are not yet committed.
func (db *database) ReplicateTentativeRecord(record *proto.Record, committedRevision int64) (*proto.Record, error) {
	// Committed records cannot be overwritten — delegate to the standard
	// idempotent-only path.
	if record.Revision <= committedRevision {
		return db.ReplicateRecord(record)
	}

	// Tentative record — attempt normal insert first.
	inserted, err := db.ReplicateRecord(record)
	if err == nil {
		return inserted, nil
	}

	// If the error is not a content mismatch conflict, propagate it.
	if !errors.Is(err, ErrReplicateConflict) {
		return nil, err
	}

	// Tentative record conflict — overwrite by deleting the old row and
	// re-inserting with the new contents.
	if _, delErr := db.conn.Exec("DELETE FROM records WHERE revision = ?", record.Revision); delErr != nil {
		return nil, fmt.Errorf("delete tentative record revision %d: %w", record.Revision, delErr)
	}

	return db.ReplicateRecord(record)
}
