// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"bytes"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestReplicateTentativeRecordInsertNoConflict verifies that a tentative record
// is inserted normally when no existing record is present.
func TestReplicateTentativeRecordInsertNoConflict(t *testing.T) {
	db := openTestDB(t)

	record := &proto.Record{
		Revision:       5,
		Key:            []byte("key-a"),
		Created:        true,
		Version:        1,
		CreateRevision: 5,
		CreatedAt:      timestamppb.New(time.Unix(5, 0).UTC()),
		LeaderId:       "leader-1",
		Value:          []byte("value-a"),
	}

	inserted, err := db.ReplicateTentativeRecord(record, 0)
	if err != nil {
		t.Fatalf("ReplicateTentativeRecord() error = %v", err)
	}
	if inserted.GetRevision() != 5 {
		t.Fatalf("revision = %d, want 5", inserted.GetRevision())
	}
}

// TestReplicateTentativeRecordOverwriteAboveCommitted verifies that a tentative
// record (above committed_revision) with different contents is overwritten.
func TestReplicateTentativeRecordOverwriteAboveCommitted(t *testing.T) {
	db := openTestDB(t)

	// Insert initial tentative record.
	insertReplicated(t, db, &proto.Record{
		Revision:       10,
		Key:            []byte("key-b"),
		Created:        true,
		Version:        1,
		CreateRevision: 10,
		CreatedAt:      timestamppb.New(time.Unix(10, 0).UTC()),
		LeaderId:       "leader-1",
		Value:          []byte("original-value"),
	})

	// Overwrite with a different record at the same revision.
	// committed_revision is 5, so revision 10 is tentative.
	replacement := &proto.Record{
		Revision:       10,
		Key:            []byte("key-b"),
		Created:        true,
		Version:        1,
		CreateRevision: 10,
		CreatedAt:      timestamppb.New(time.Unix(10, 0).UTC()),
		LeaderId:       "leader-1",
		Value:          []byte("replacement-value"),
	}

	inserted, err := db.ReplicateTentativeRecord(replacement, 5)
	if err != nil {
		t.Fatalf("ReplicateTentativeRecord() error = %v", err)
	}
	if !bytes.Equal(inserted.GetValue(), []byte("replacement-value")) {
		t.Fatalf("value = %q, want %q", inserted.GetValue(), "replacement-value")
	}

	// Verify exactly one record exists.
	count, err := db.RecordCount()
	if err != nil {
		t.Fatalf("RecordCount() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("RecordCount() = %d, want 1", count)
	}
}

// TestReplicateTentativeRecordCommittedNotOverwritten verifies that a committed
// record (at or below committed_revision) with mismatching contents is rejected.
func TestReplicateTentativeRecordCommittedNotOverwritten(t *testing.T) {
	db := openTestDB(t)

	insertReplicated(t, db, &proto.Record{
		Revision:       3,
		Key:            []byte("key-c"),
		Created:        true,
		Version:        1,
		CreateRevision: 3,
		CreatedAt:      timestamppb.New(time.Unix(3, 0).UTC()),
		LeaderId:       "leader-1",
		Value:          []byte("committed-value"),
	})

	// Attempt to overwrite with different contents, but revision 3 is at or
	// below committed_revision 5, so this must fail.
	_, err := db.ReplicateTentativeRecord(&proto.Record{
		Revision:       3,
		Key:            []byte("key-c"),
		Created:        true,
		Version:        1,
		CreateRevision: 3,
		CreatedAt:      timestamppb.New(time.Unix(3, 0).UTC()),
		LeaderId:       "leader-1",
		Value:          []byte("different-value"),
	}, 5)

	if err == nil {
		t.Fatal("expected error when overwriting committed record with different contents")
	}
}

// TestReplicateTentativeRecordCommittedIdempotent verifies that a committed
// record with identical contents is accepted (idempotent duplicate).
func TestReplicateTentativeRecordCommittedIdempotent(t *testing.T) {
	db := openTestDB(t)

	record := &proto.Record{
		Revision:       3,
		Key:            []byte("key-d"),
		Created:        true,
		Version:        1,
		CreateRevision: 3,
		CreatedAt:      timestamppb.New(time.Unix(3, 0).UTC()),
		LeaderId:       "leader-1",
		Value:          []byte("stable-value"),
	}

	insertReplicated(t, db, record)

	// Same record again with committed_revision >= revision. Idempotent.
	inserted, err := db.ReplicateTentativeRecord(record, 5)
	if err != nil {
		t.Fatalf("ReplicateTentativeRecord() error = %v", err)
	}
	if inserted.GetRevision() != 3 {
		t.Fatalf("revision = %d, want 3", inserted.GetRevision())
	}
}
