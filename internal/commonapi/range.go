// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package commonapi

import (
	"bytes"
	"context"
	"math"

	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/proto"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
)

// RangeStream executes a range request as a server-side stream
func RangeStream(db localdb.Database, ctx context.Context, r *pb.RangeRequest, committedRevision int64, send func(*pb.RangeStreamResponse) error) error {
	if err := validateRangeRequest(r); err != nil {
		return err
	}

	// RangeStream only supports the default ordering
	if r.SortOrder != pb.RangeRequest_NONE {
		return status.Errorf(codes.Unimplemented, "custom sort orders not supported")
	}

	sizer := newChunkSizer()
	queryWhere, queryArgs := buildRangeQuery(r)

	// CountOnly is returned as a single response
	if r.CountOnly {
		_, totalCount, err := db.FindRecordsBy(queryWhere, queryArgs, r.Revision, 1, "ASC")
		if err != nil {
			return err
		}

		return send(&pb.RangeStreamResponse{
			RangeResponse: &pb.RangeResponse{
				Header: &pb.ResponseHeader{Revision: committedRevision},
				Count:  totalCount,
				More:   r.Limit > 0 && totalCount > r.Limit,
			},
		})
	}

	// Limit is the total number of records returned across the stream
	totalLimit := r.Limit
	if totalLimit == 0 {
		totalLimit = math.MaxInt64
	}

	var count int64
	var afterKey []byte

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		chunkLimit := min(sizer.Keys(), totalLimit-count)

		// Ask for one extra record so we know whether another chunk exists
		rows, err := db.FindRecordsForRangeStream(queryWhere, queryArgs, r.Revision, chunkLimit+1, "ASC", afterKey)
		if err != nil {
			return err
		}

		more := int64(len(rows)) > chunkLimit
		if more {
			rows = rows[:chunkLimit]
		}

		kvs, err := recordsToKeyValues(rows)
		if err != nil {
			return err
		}

		count += int64(len(kvs))

		done := !more || count == totalLimit

		resp := &pb.RangeResponse{
			Kvs: kvs,
		}
		sizer.Observe(int64(gproto.Size(resp)))

		// Like etcd, only the final response contains the header, count, and More fields
		if done {
			total := count

			// When the limit truncated the result, unary Range still reports the
			// total number of keys in the range, so do the same. This runs once,
			// only for truncated requests. A full stream (limit 0) never pays for it.
			if more {
				_, total, err = db.FindRecordsBy(queryWhere, queryArgs, r.Revision, 1, "ASC")
				if err != nil {
					return err
				}
			}
			resp.Header = &pb.ResponseHeader{
				Revision: committedRevision,
			}
			resp.Count = total
			resp.More = more
		}

		if err := send(&pb.RangeStreamResponse{
			RangeResponse: resp,
		}); err != nil {
			return err
		}

		if done {
			return nil
		}

		// Continue after the last key from this chunk
		afterKey = kvs[len(kvs)-1].Key
	}
}

// Range executes the shared Range request logic against the local database.
//
// The committedRevision argument is the cluster's current committed revision
// at the moment the caller dispatched the request; it is placed verbatim in
// the response Header.Revision. This mirrors etcd, where Header.Revision is
// always the store's current revision — not the maximum revision among the
// matched records.
func Range(db localdb.Database, ctx context.Context, r *pb.RangeRequest, committedRevision int64) (*pb.RangeResponse, error) {
	// check if an unsupported option was specified
	if err := validateRangeRequest(r); err != nil {
		return nil, err
	}

	// determine query where criteria and args
	queryWhere, queryArgs := buildRangeQuery(r)

	// determine sort order
	order := "ASC"
	if r.SortOrder == pb.RangeRequest_DESCEND {
		order = "DESC"
	}

	// query data with count
	var kvs []*mvccpb.KeyValue
	rows, totalCount, err := db.FindRecordsBy(queryWhere, queryArgs, r.Revision, r.Limit, order)
	if err != nil {
		return nil, err
	}

	// determine if there are more results
	more := totalCount > int64(len(rows))

	if r.CountOnly {
		return &pb.RangeResponse{
			Header: &pb.ResponseHeader{
				Revision: committedRevision,
			},
			Count: totalCount,
			More:  more,
		}, nil
	}

	// process results and return response
	if kvs, err = recordsToKeyValues(rows); err != nil {
		return nil, err
	}

	return &pb.RangeResponse{
		Header: &pb.ResponseHeader{
			Revision: committedRevision,
		},
		Kvs:   kvs,
		Count: totalCount,
		More:  more,
	}, nil
}

// validateRangeRequest rejects RangeRequest options that Range and
// RangeStream don't support
func validateRangeRequest(r *pb.RangeRequest) error {
	if r.Limit < 0 {
		return status.Errorf(codes.InvalidArgument, "limit must be non-negative")
	}
	if r.KeysOnly {
		return status.Errorf(codes.Unimplemented, "keys_only not supported")
	} else if r.MaxCreateRevision != 0 {
		return status.Errorf(codes.Unimplemented, "max_create_revision not supported")
	} else if r.MaxModRevision != 0 {
		return status.Errorf(codes.Unimplemented, "max_mod_revision not supported")
	} else if r.MinModRevision != 0 {
		return status.Errorf(codes.Unimplemented, "min_mod_revision not supported")
	} else if r.MinCreateRevision != 0 {
		return status.Errorf(codes.Unimplemented, "min_create_revision not supported")
	} else if r.Serializable {
		return status.Errorf(codes.Unimplemented, "serializable not supported")
	} else if r.SortTarget != 0 {
		return status.Errorf(codes.Unimplemented, "sort_target not supported")
	}
	return nil
}

// recordsToKeyValues converts stored records into etcd KeyValues, returning
// ErrGRPCCompacted if any record has been compacted
func recordsToKeyValues(rows []*proto.Record) ([]*mvccpb.KeyValue, error) {
	kvs := make([]*mvccpb.KeyValue, 0, len(rows))

	for _, row := range rows {
		if row.CompactedAt != nil {
			return nil, rpctypes.ErrGRPCCompacted
		}

		kvs = append(kvs, &mvccpb.KeyValue{
			Key:            row.Key,
			CreateRevision: row.CreateRevision,
			ModRevision:    row.Revision,
			Value:          row.Value,
			Version:        row.Version,
			Lease:          row.Lease,
		})
	}

	return kvs, nil
}

// buildRangeQuery translates a RangeRequest's key/range_end into a SQL WHERE
// clause and its arguments
func buildRangeQuery(r *pb.RangeRequest) (string, []any) {
	// TODO: similar to watch.Go isInRange, consider refactor
	zeroByte := []byte{0}
	keyAndZeroByte := append(r.Key, byte(0))
	keyCopy := make([]byte, len(r.Key))
	copy(keyCopy, r.Key)
	rangeEndPrefixValue := append(
		keyCopy[:len(keyCopy)-1],
		keyCopy[len(keyCopy)-1]+1,
	)
	var queryWhere string
	var queryArgs []any
	if len(r.RangeEnd) == 0 || bytes.Equal(r.RangeEnd, keyAndZeroByte) {
		// exact match
		// key = r.Key
		queryWhere = "key = ?"
		queryArgs = []any{r.Key}
	} else if bytes.Equal(r.Key, zeroByte) && bytes.Equal(r.RangeEnd, zeroByte) {
		// both keys are zero bytes, return all keys
		// no WHERE
	} else if bytes.Equal(r.RangeEnd, zeroByte) {
		// rangeEnd is zero bytes, get all keys greater than or equal to r.Key
		// key > r.Key
		queryWhere = "key >= ?"
		queryArgs = []any{r.Key}
	} else if bytes.Equal(r.RangeEnd, rangeEndPrefixValue) {
		// get all keys matching prefix, where key is the prefix
		// this is invoked by sending key+1 byte as rangeEnd
		// per the docs:
		// "If range_end is key plus one
		// (e.g., “aa”+1 == “ab”, “a\xff”+1 == “b”),
		// then the range represents all keys prefixed with key."
		// key LIKE prefix%
		queryWhere = "key LIKE ?"
		queryArgs = []any{append(r.Key, byte(37))}
	} else {
		// range; get all keys from r.Key to less than r.RangeEnd
		// key >= r.Key
		// AND key < r.RangeEnd
		queryWhere = "key >= ? AND key < ?"
		queryArgs = []any{r.Key, r.RangeEnd}
	}

	return queryWhere, queryArgs
}
