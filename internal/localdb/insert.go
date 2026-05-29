// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/netsy-dev/netsy/internal/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Define Err for InsertRecord
var ErrCompareRevisionFailed = errors.New("compare failed: revision mismatch")
var ErrCreateKeyExists = errors.New("cannot create record: key exists")
var ErrDeleteKeyNotFound = errors.New("cannot delete record: key does not exist")

// InsertRecord is used for all transactions including create, update, and
// delete operations. If tx is provided, the operation will be performed within
// that transaction; otherwise, it uses the database connection directly.
//
// This uses a custom query and uses a Common Table Expression (CTE) to retrieve
// the existing latest revision for the key and for the table in order to
// execute atomically.
//
// For create requests (created=true), we require CTE latest_revision_for_key
// to either not exist, or exist as deleted. Otherwise we set created to null,
// which violates a NOT NULL constraint and therefore fails the query,
// and returns ErrCreateKeyExists.
//
// For delete requests (deleted=true), we require CTE latest_revision_for_key
// to exist and have deleted=false. Otherwise we set deleted to null, which
// violates a NOT NULL constraint and therefore fails the query, and returns
// ErrDeleteKeyNotFound.
//
// For all requests, prevRevision must be set (but can be 0), and we require
// CTE latest_revision_for_key to exist and match prevRevision, or otherwise
// set prev_revision to null, which violates a NOT NULL constraint and
// therefore fails the query, and returns ErrCompareRevisionFailed.
func (db *database) InsertRecord(record *proto.Record, tx *Tx) (*proto.Record, error) {
	// validate data
	if record.Revision <= 0 ||
		len(record.Key) == 0 ||
		record.PrevRevision < 0 || // optional, compare mod_revision
		record.Lease < 0 || // optional
		record.Dek < 0 || // optional
		record.CreateRevision != 0 ||
		record.Version != 0 ||
		record.CreatedAt != nil ||
		record.CompactedAt != nil ||
		record.LeaderId == "" ||
		record.ReplicatedAt != nil ||
		record.Crc != 0 ||
		(record.Created && record.Deleted) {
		return nil, fmt.Errorf("invalid record data for insert")
	}

	// Set created at
	record.CreatedAt = timestamppb.Now()

	// Determine which query interface to use
	var queryInterface interface {
		QueryRow(query string, args ...any) *sql.Row
	}
	if tx != nil {
		queryInterface = tx.tx
	} else {
		queryInterface = db.conn
	}

	// insert record and get returned values
	var returnedRecord proto.Record
	var createdAtStr string
	var compactedAtStr, replicatedAtStr sql.NullString
	err := queryInterface.QueryRow(
		insertRecordSQL,
		record.Revision,     // ?1
		record.Key,          // ?2
		record.Created,      // ?3
		record.Deleted,      // ?4
		record.PrevRevision, // ?5
		record.Lease,        // ?6
		record.Dek,          // ?7
		record.Value,        // ?8
		record.CreatedAt.AsTime().Format(time.RFC3339Nano), // ?9
		record.LeaderId, // ?10
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
		&createdAtStr,
		&compactedAtStr,
		&returnedRecord.LeaderId,
		&replicatedAtStr,
	)
	if err != nil && err.Error() == "NOT NULL constraint failed: records.created" {
		return nil, ErrCreateKeyExists
	} else if err != nil && err.Error() == "NOT NULL constraint failed: records.deleted" {
		return nil, ErrDeleteKeyNotFound
	} else if err != nil && err.Error() == "NOT NULL constraint failed: records.prev_revision" {
		return nil, ErrCompareRevisionFailed
	} else if err != nil {
		return nil, err
	}

	if returnedRecord.Revision < 1 {
		return nil, fmt.Errorf("Unexpected error: insert ID (%d) invalid", returnedRecord.Revision)
	}

	// Convert string timestamps back to protobuf timestamps
	if createdAtStr != "" {
		if t, err := time.Parse(time.RFC3339Nano, createdAtStr); err == nil {
			returnedRecord.CreatedAt = timestamppb.New(t)
		}
	}
	if compactedAtStr.Valid && compactedAtStr.String != "" {
		if t, err := time.Parse(time.RFC3339Nano, compactedAtStr.String); err == nil {
			returnedRecord.CompactedAt = timestamppb.New(t)
		}
	}
	if replicatedAtStr.Valid && replicatedAtStr.String != "" {
		if t, err := time.Parse(time.RFC3339Nano, replicatedAtStr.String); err == nil {
			returnedRecord.ReplicatedAt = timestamppb.New(t)
		}
	}

	return &returnedRecord, nil
}

const insertRecordSQL = `
  WITH
  latest_revision_for_key AS (
    SELECT revision,deleted,create_revision,version,value
    FROM records
    WHERE key = ?2
    ORDER BY revision DESC
    LIMIT 1
  ),
  latest_revision_for_table AS (
    SELECT COALESCE(MAX(revision), 0) as revision FROM records
  )
  INSERT INTO records (
    revision,
    key,
    created,
    deleted,
    create_revision,
    prev_revision,
    version,
    lease,
    dek,
    value,
    created_at,
    compacted_at,
    leader_id,
    replicated_at
  )
  SELECT
    /* revision */
    ?1,
    /* key */
    ?2,
    /* created */
    CASE WHEN ?3 = 1
    THEN
	    CASE
	        WHEN (SELECT deleted FROM latest_revision_for_key) = 0
	        THEN
	            null
	        ELSE
	            1
	    END
    ELSE
        0
    END,
    /* deleted */
    CASE WHEN ?4 = 1
    THEN
	    CASE WHEN (SELECT deleted FROM latest_revision_for_key) = 0
	    THEN
	        1
	    ELSE
	        null
	    END
    ELSE
        0
    END,
    /* create_revision */
    COALESCE(
        (SELECT create_revision FROM latest_revision_for_key WHERE deleted = 0),
        (SELECT revision+1 FROM latest_revision_for_table)
    ),
    /* prev_revision */
    CASE WHEN ?5 > 0
    THEN
        CASE WHEN ?5 = IFNULL(
            (SELECT revision FROM latest_revision_for_key WHERE deleted = 0),
            0
        )
        THEN
	        ?5
	    ELSE
	        null
        END
    ELSE
        CASE WHEN IFNULL(
            (SELECT revision FROM latest_revision_for_key WHERE deleted = 0),
            0
        ) > 0
        THEN
	        null
	    ELSE
	        0
	    END
    END,
    /* version */
    CASE WHEN ?4 = 1
    THEN
        0
    ELSE
        IFNULL(
            (SELECT version FROM latest_revision_for_key),
            0
        )+1
    END,
    /* lease */
    ?6,
    /* dek */
    ?7,
    /* value */
    ?8,
    /* created_at */
    ?9,
    /* compacted_at */
    NULL,
    /* leader_id */
    ?10,
    /* replicated_at */
    NULL
  RETURNING *
`
