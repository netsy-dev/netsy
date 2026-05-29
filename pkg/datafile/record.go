// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datafile

import (
	"time"

	pb "github.com/netsy-dev/netsy/internal/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Record is a Netsy key-value record stored in a .netsy data file.
type Record struct {
	Revision       int64
	Key            []byte
	Created        bool
	Deleted        bool
	CreateRevision int64
	PrevRevision   int64
	Version        int64
	Lease          int64
	Dek            int64
	Value          []byte
	CreatedAt      *time.Time
	CompactedAt    *time.Time
	LeaderID       string
	ReplicatedAt   *time.Time
}

func recordFromProto(record *pb.Record) *Record {
	return &Record{
		Revision:       record.Revision,
		Key:            record.Key,
		Created:        record.Created,
		Deleted:        record.Deleted,
		CreateRevision: record.CreateRevision,
		PrevRevision:   record.PrevRevision,
		Version:        record.Version,
		Lease:          record.Lease,
		Dek:            record.Dek,
		Value:          record.Value,
		CreatedAt:      timeFromProto(record.CreatedAt),
		CompactedAt:    timeFromProto(record.CompactedAt),
		LeaderID:       record.LeaderId,
		ReplicatedAt:   timeFromProto(record.ReplicatedAt),
	}
}

func recordToProto(record *Record) *pb.Record {
	return &pb.Record{
		Revision:       record.Revision,
		Key:            record.Key,
		Created:        record.Created,
		Deleted:        record.Deleted,
		CreateRevision: record.CreateRevision,
		PrevRevision:   record.PrevRevision,
		Version:        record.Version,
		Lease:          record.Lease,
		Dek:            record.Dek,
		Value:          record.Value,
		CreatedAt:      timeToProto(record.CreatedAt),
		CompactedAt:    timeToProto(record.CompactedAt),
		LeaderId:       record.LeaderID,
		ReplicatedAt:   timeToProto(record.ReplicatedAt),
	}
}

func timeFromProto(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := ts.AsTime()
	return &t
}

func timeToProto(t *time.Time) *timestamppb.Timestamp {
	if t == nil {
		return nil
	}
	return timestamppb.New(*t)
}
