// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package localdb

import (
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestFindRecordsByWithoutFilterReturnsAllKeys(t *testing.T) {
	db := openTestDB(t)

	for revision, key := range []string{"alpha", "beta"} {
		insertReplicated(t, db, &proto.Record{
			Revision:       int64(revision + 1),
			Key:            []byte(key),
			Created:        true,
			CreateRevision: int64(revision + 1),
			Version:        1,
			CreatedAt:      timestamppb.New(time.Unix(int64(revision+1), 0).UTC()),
			LeaderId:       "leader-1",
		})
	}

	records, count, err := db.FindRecordsBy("", nil, 0, 0, "ASC")
	if err != nil {
		t.Fatalf("FindRecordsBy() error = %v", err)
	}
	if count != 2 {
		t.Fatalf("FindRecordsBy() count = %d, want 2", count)
	}
	if len(records) != 2 {
		t.Fatalf("FindRecordsBy() returned %d records, want 2", len(records))
	}
	if string(records[0].GetKey()) != "alpha" || string(records[1].GetKey()) != "beta" {
		t.Fatalf("FindRecordsBy() keys = [%q, %q], want [alpha, beta]", records[0].GetKey(), records[1].GetKey())
	}
}
