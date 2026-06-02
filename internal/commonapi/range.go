// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package commonapi

import (
	"bytes"
	"context"

	"github.com/netsy-dev/netsy/internal/localdb"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Range executes the shared Range request logic against the local database.
//
// The committedRevision argument is the cluster's current committed revision
// at the moment the caller dispatched the request; it is placed verbatim in
// the response Header.Revision. This mirrors etcd, where Header.Revision is
// always the store's current revision — not the maximum revision among the
// matched records.
func Range(db localdb.Database, ctx context.Context, r *pb.RangeRequest, committedRevision int64) (*pb.RangeResponse, error) {
	// check if an unsupported option was specified
	if r.KeysOnly {
		return nil, status.Errorf(codes.Unimplemented, "keys_only not supported")
	} else if r.MaxCreateRevision != 0 {
		return nil, status.Errorf(codes.Unimplemented, "max_create_revision not supported")
	} else if r.MaxModRevision != 0 {
		return nil, status.Errorf(codes.Unimplemented, "max_mod_revision not supported")
	} else if r.MinModRevision != 0 {
		return nil, status.Errorf(codes.Unimplemented, "min_mod_revision not supported")
	} else if r.MinCreateRevision != 0 {
		return nil, status.Errorf(codes.Unimplemented, "min_create_revision not supported")
	} else if r.Serializable {
		return nil, status.Errorf(codes.Unimplemented, "serializable not supported")
	} else if r.SortTarget != 0 {
		return nil, status.Errorf(codes.Unimplemented, "sort_target not supported")
	}

	// validate options
	if r.Limit < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "limit must be non-negative")
	}

	// determine query where criteria and args
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
	kvs = []*mvccpb.KeyValue{}
	for _, row := range rows {
		if row.CompactedAt != nil {
			return nil, rpctypes.ErrGRPCCompacted
		}
		kvs = append(kvs,
			&mvccpb.KeyValue{
				Key:            row.Key,
				CreateRevision: row.CreateRevision,
				ModRevision:    row.Revision,
				Value:          row.Value,
				Version:        row.Version,
				Lease:          row.Lease,
			},
		)
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
