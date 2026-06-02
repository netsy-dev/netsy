// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestDeriveCompactionRevision(t *testing.T) {
	db := openTestDB(t)

	insertReplicated(t, db, &proto.Record{
		Revision:       1,
		Key:            []byte("a"),
		Created:        true,
		Version:        1,
		CreateRevision: 1,
		CreatedAt:      timestamppb.New(time.Unix(1, 0).UTC()),
		CompactedAt:    timestamppb.New(time.Unix(10, 0).UTC()),
		LeaderId:       "leader-1",
	})
	insertReplicated(t, db, &proto.Record{
		Revision:       2,
		Key:            []byte("b"),
		Created:        true,
		Version:        1,
		CreateRevision: 2,
		CreatedAt:      timestamppb.New(time.Unix(2, 0).UTC()),
		CompactedAt:    timestamppb.New(time.Unix(11, 0).UTC()),
		LeaderId:       "leader-1",
	})
	insertReplicated(t, db, &proto.Record{
		Revision:       3,
		Key:            []byte("c"),
		Created:        true,
		Version:        1,
		CreateRevision: 3,
		CreatedAt:      timestamppb.New(time.Unix(3, 0).UTC()),
		LeaderId:       "leader-1",
	})

	got, err := db.DeriveCompactionRevision()
	if err != nil {
		t.Fatalf("DeriveCompactionRevision() error = %v", err)
	}
	if got != 2 {
		t.Fatalf("DeriveCompactionRevision() = %d, want 2", got)
	}
}

func TestDeriveCompactionRevisionAllCompacted(t *testing.T) {
	db := openTestDB(t)

	insertReplicated(t, db, &proto.Record{
		Revision:       1,
		Key:            []byte("a"),
		Created:        true,
		Version:        1,
		CreateRevision: 1,
		CreatedAt:      timestamppb.New(time.Unix(1, 0).UTC()),
		CompactedAt:    timestamppb.New(time.Unix(10, 0).UTC()),
		LeaderId:       "leader-1",
	})
	insertReplicated(t, db, &proto.Record{
		Revision:       2,
		Key:            []byte("b"),
		Created:        true,
		Version:        1,
		CreateRevision: 2,
		CreatedAt:      timestamppb.New(time.Unix(2, 0).UTC()),
		CompactedAt:    timestamppb.New(time.Unix(11, 0).UTC()),
		LeaderId:       "leader-1",
	})

	got, err := db.DeriveCompactionRevision()
	if err != nil {
		t.Fatalf("DeriveCompactionRevision() error = %v", err)
	}
	if got != 2 {
		t.Fatalf("DeriveCompactionRevision() = %d, want 2", got)
	}
}

func TestPersistCompactionRevisionIdempotent(t *testing.T) {
	db := openTestDB(t)

	if err := db.PersistCompactionRevision(12); err != nil {
		t.Fatalf("PersistCompactionRevision(12) first error = %v", err)
	}
	if err := db.PersistCompactionRevision(12); err != nil {
		t.Fatalf("PersistCompactionRevision(12) second error = %v", err)
	}

	got, err := db.LatestCompactionRevision()
	if err != nil {
		t.Fatalf("LatestCompactionRevision() error = %v", err)
	}
	if got != 12 {
		t.Fatalf("LatestCompactionRevision() = %d, want 12", got)
	}
}

func TestExecuteCompaction(t *testing.T) {
	type compactionRecord struct {
		*proto.Record
		expectCompaction bool
	}

	tests := []struct {
		name               string
		compactionRevision int64
		records            []compactionRecord
	}{
		{
			name:               "mixed records",
			compactionRevision: 4,
			records: []compactionRecord{
				{
					Record: &proto.Record{
						Revision:       1,
						Key:            []byte("a"),
						Created:        true,
						Version:        1,
						CreateRevision: 1,
						PrevRevision:   0,
						CreatedAt:      timestamppb.New(time.Unix(1, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("val-a"),
					},
					expectCompaction: true,
				},
				{
					Record: &proto.Record{
						Revision:       2,
						Key:            []byte("a"),
						Version:        2,
						CreateRevision: 1,
						PrevRevision:   1,
						CreatedAt:      timestamppb.New(time.Unix(2, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("val-a2"),
					},
					expectCompaction: true,
				},
				{
					Record: &proto.Record{
						Revision:       3,
						Key:            []byte("d"),
						Created:        true,
						Version:        1,
						CreateRevision: 3,
						PrevRevision:   0,
						CreatedAt:      timestamppb.New(time.Unix(3, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("val-d"),
					},
					expectCompaction: true,
				},
				{
					Record: &proto.Record{
						Revision:       4,
						Key:            []byte("d"),
						Deleted:        true,
						Version:        2,
						CreateRevision: 3,
						PrevRevision:   3,
						CreatedAt:      timestamppb.New(time.Unix(4, 0).UTC()),
						LeaderId:       "leader-1",
					},
					expectCompaction: true,
				},
				{
					Record: &proto.Record{
						Revision:       5,
						Key:            []byte("b"),
						Created:        true,
						Version:        1,
						CreateRevision: 5,
						PrevRevision:   0,
						CreatedAt:      timestamppb.New(time.Unix(5, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("val-b"),
					},
				},
				{
					Record: &proto.Record{
						Revision:       6,
						Key:            []byte("c"),
						Created:        true,
						Version:        1,
						CreateRevision: 6,
						PrevRevision:   0,
						CreatedAt:      timestamppb.New(time.Unix(6, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("val-c"),
					},
				},
				{
					Record: &proto.Record{
						Revision:       7,
						Key:            []byte("a"),
						Version:        3,
						CreateRevision: 1,
						PrevRevision:   2,
						CreatedAt:      timestamppb.New(time.Unix(7, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("val-a3"),
					},
				},
			},
		},
		{
			name:               "tombstone compacted when key is recreated after deletion",
			compactionRevision: 10,
			records: []compactionRecord{
				{
					Record: &proto.Record{
						Revision:       1,
						Key:            []byte("a"),
						Created:        true,
						Version:        1,
						CreateRevision: 1,
						PrevRevision:   0,
						CreatedAt:      timestamppb.New(time.Unix(1, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("hello"),
					},
					expectCompaction: true,
				},
				{
					Record: &proto.Record{
						Revision:       5,
						Key:            []byte("a"),
						Deleted:        true,
						Version:        2,
						CreateRevision: 1,
						PrevRevision:   1,
						CreatedAt:      timestamppb.New(time.Unix(5, 0).UTC()),
						LeaderId:       "leader-1",
					},
					expectCompaction: true,
				},
				{
					Record: &proto.Record{
						Revision:       11,
						Key:            []byte("a"),
						Created:        true,
						Version:        1,
						CreateRevision: 11,
						PrevRevision:   5,
						CreatedAt:      timestamppb.New(time.Unix(11, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("b"),
					},
				},
			},
		},
		{
			name:               "latest tombstone compacted",
			compactionRevision: 10,
			records: []compactionRecord{
				{
					Record: &proto.Record{
						Revision:       1,
						Key:            []byte("a"),
						Created:        true,
						Version:        1,
						CreateRevision: 1,
						PrevRevision:   0,
						CreatedAt:      timestamppb.New(time.Unix(1, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("hello"),
					},
					expectCompaction: true,
				},
				{
					Record: &proto.Record{
						Revision:       5,
						Key:            []byte("a"),
						Deleted:        true,
						Version:        2,
						CreateRevision: 1,
						PrevRevision:   1,
						CreatedAt:      timestamppb.New(time.Unix(5, 0).UTC()),
						LeaderId:       "leader-1",
					},
					expectCompaction: true,
				},
			},
		},
		{
			name:               "value compacted when deleted after compaction revision",
			compactionRevision: 10,
			records: []compactionRecord{
				{
					Record: &proto.Record{
						Revision:       5,
						Key:            []byte("a"),
						Created:        true,
						Version:        1,
						CreateRevision: 5,
						PrevRevision:   0,
						CreatedAt:      timestamppb.New(time.Unix(5, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("hello"),
					},
					expectCompaction: true,
				},
				{
					Record: &proto.Record{
						Revision:       11,
						Key:            []byte("a"),
						Deleted:        true,
						Version:        2,
						CreateRevision: 5,
						PrevRevision:   5,
						CreatedAt:      timestamppb.New(time.Unix(11, 0).UTC()),
						LeaderId:       "leader-1",
					},
				},
			},
		},
		{
			name:               "latest value before compaction revision preserved",
			compactionRevision: 10,
			records: []compactionRecord{
				{
					Record: &proto.Record{
						Revision:       5,
						Key:            []byte("a"),
						Created:        true,
						Version:        1,
						CreateRevision: 5,
						PrevRevision:   0,
						CreatedAt:      timestamppb.New(time.Unix(5, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("hello"),
					},
				},
			},
		},
		{
			name:               "newer record for different key does not compact value",
			compactionRevision: 10,
			records: []compactionRecord{
				{
					Record: &proto.Record{
						Revision:       5,
						Key:            []byte("a"),
						Created:        true,
						Version:        1,
						CreateRevision: 5,
						PrevRevision:   0,
						CreatedAt:      timestamppb.New(time.Unix(5, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("hello"),
					},
				},
				{
					Record: &proto.Record{
						Revision:       11,
						Key:            []byte("b"),
						Created:        true,
						Version:        1,
						CreateRevision: 11,
						PrevRevision:   0,
						CreatedAt:      timestamppb.New(time.Unix(11, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("world"),
					},
				},
			},
		},
		{
			name:               "value at compaction revision compacted when superseded later",
			compactionRevision: 10,
			records: []compactionRecord{
				{
					Record: &proto.Record{
						Revision:       10,
						Key:            []byte("a"),
						Created:        true,
						Version:        1,
						CreateRevision: 10,
						PrevRevision:   0,
						CreatedAt:      timestamppb.New(time.Unix(10, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("hello"),
					},
					expectCompaction: true,
				},
				{
					Record: &proto.Record{
						Revision:       11,
						Key:            []byte("a"),
						Version:        2,
						CreateRevision: 10,
						PrevRevision:   10,
						CreatedAt:      timestamppb.New(time.Unix(11, 0).UTC()),
						LeaderId:       "leader-1",
						Value:          []byte("world"),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := openTestDB(t)

			var wantAffected int64
			for _, test := range tt.records {
				insertReplicated(t, db, test.Record)
				if test.expectCompaction {
					wantAffected++
				}
			}

			affected, err := db.ExecuteCompaction(tt.compactionRevision)
			if err != nil {
				t.Fatalf("ExecuteCompaction(%d) error = %v", tt.compactionRevision, err)
			}
			if affected != wantAffected {
				t.Fatalf("ExecuteCompaction(%d) affected = %d, want %d", tt.compactionRevision, affected, wantAffected)
			}

			for _, test := range tt.records {
				record, err := db.FindRecordByRev(test.Revision)
				if err != nil {
					t.Fatalf("FindRecordByRev(%d) error = %v", test.Revision, err)
				}
				if test.expectCompaction {
					if record.Value != nil {
						t.Fatalf("revision %d value = %v, want nil", test.Revision, record.Value)
					}
					if record.CompactedAt == nil {
						t.Fatalf("revision %d compacted_at is nil, want set", test.Revision)
					}
				} else {
					if string(record.Value) != string(test.Value) {
						t.Fatalf("revision %d value = %q, want %q", test.Revision, record.Value, test.Value)
					}
					if record.CompactedAt != nil {
						t.Fatalf("revision %d compacted_at = %v, want nil", test.Revision, record.CompactedAt)
					}
				}
				if record.Version != test.Version {
					t.Fatalf("revision %d version = %d, want %d", test.Revision, record.Version, test.Version)
				}
				if record.CreateRevision != test.CreateRevision {
					t.Fatalf("revision %d create_revision = %d, want %d", test.Revision, record.CreateRevision, test.CreateRevision)
				}
				if record.PrevRevision != test.PrevRevision {
					t.Fatalf("revision %d prev_revision = %d, want %d", test.Revision, record.PrevRevision, test.PrevRevision)
				}
			}

			// Running again should be idempotent (0 affected).
			affected, err = db.ExecuteCompaction(tt.compactionRevision)
			if err != nil {
				t.Fatalf("ExecuteCompaction(%d) second call error = %v", tt.compactionRevision, err)
			}
			if affected != 0 {
				t.Fatalf("ExecuteCompaction(%d) second call affected = %d, want 0", tt.compactionRevision, affected)
			}
		})
	}
}

func TestReplicateRecordDuplicateIsIdempotentAndPreservesCompactedAt(t *testing.T) {
	db := openTestDB(t)

	record := &proto.Record{
		Revision:       7,
		Key:            []byte("key"),
		Created:        true,
		Version:        1,
		CreateRevision: 7,
		CreatedAt:      timestamppb.New(time.Unix(7, 0).UTC()),
		CompactedAt:    timestamppb.New(time.Unix(17, 0).UTC()),
		LeaderId:       "leader-1",
	}

	inserted, err := db.ReplicateRecord(record)
	if err != nil {
		t.Fatalf("first ReplicateRecord() error = %v", err)
	}
	if inserted.GetCompactedAt() == nil {
		t.Fatal("first ReplicateRecord() did not persist compacted_at")
	}

	duplicated, err := db.ReplicateRecord(record)
	if err != nil {
		t.Fatalf("second ReplicateRecord() error = %v", err)
	}
	if duplicated.GetRevision() != record.GetRevision() {
		t.Fatalf("duplicate ReplicateRecord() revision = %d, want %d", duplicated.GetRevision(), record.GetRevision())
	}
	if duplicated.GetCompactedAt() == nil || !duplicated.GetCompactedAt().AsTime().Equal(record.GetCompactedAt().AsTime()) {
		t.Fatal("duplicate ReplicateRecord() lost compacted_at")
	}

	count, err := db.RecordCount()
	if err != nil {
		t.Fatalf("RecordCount() error = %v", err)
	}
	if count != 1 {
		t.Fatalf("RecordCount() = %d, want 1", count)
	}
}

func TestReplicateRecordDuplicateMismatchFails(t *testing.T) {
	db := openTestDB(t)

	insertReplicated(t, db, &proto.Record{
		Revision:       9,
		Key:            []byte("key"),
		Created:        true,
		Version:        1,
		CreateRevision: 9,
		CreatedAt:      timestamppb.New(time.Unix(9, 0).UTC()),
		LeaderId:       "leader-1",
		Value:          []byte("value-a"),
	})

	_, err := db.ReplicateRecord(&proto.Record{
		Revision:       9,
		Key:            []byte("key"),
		Created:        true,
		Version:        1,
		CreateRevision: 9,
		CreatedAt:      timestamppb.New(time.Unix(9, 0).UTC()),
		LeaderId:       "leader-1",
		Value:          []byte("value-b"),
	})
	if err == nil {
		t.Fatal("ReplicateRecord() mismatch duplicate succeeded, want error")
	}
}

func openTestDB(t *testing.T) *database {
	t.Helper()

	db := New(filepath.Join(t.TempDir(), "test.sqlite3"))
	if err := db.Connect(); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	return db
}

func insertReplicated(t *testing.T, db *database, record *proto.Record) {
	t.Helper()

	if _, err := db.ReplicateRecord(record); err != nil {
		t.Fatalf("ReplicateRecord(%d) error = %v", record.GetRevision(), err)
	}
}
