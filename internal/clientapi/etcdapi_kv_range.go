// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"context"
	"time"

	"github.com/netsy-dev/netsy/internal/commonapi"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Range serves a read request limited to the committed revision. Records
// above the committed revision are tentative and must not be visible to
// clients.
func (cs *ClientAPIServer) Range(ctx context.Context, r *pb.RangeRequest) (*pb.RangeResponse, error) {
	start := time.Now()
	committed := cs.state.Committed()

	// Limit the request revision to the committed revision so tentative
	// records above it are never served.
	if r.Revision == 0 || r.Revision > committed {
		r.Revision = committed
	}

	resp, err := commonapi.Range(cs.db, ctx, r, committed)
	if cs.metrics != nil {
		result := "success"
		if err != nil {
			result = "error"
		}
		cs.metrics.RequestsTotal.WithLabelValues("range", result).Inc()
		cs.metrics.RequestDuration.WithLabelValues("range").Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// RangeStream serves a range request as a server-side stream
func (cs *ClientAPIServer) RangeStream(r *pb.RangeRequest, rs pb.KV_RangeStreamServer) error {
	start := time.Now()
	committed := cs.state.Committed()

	// Limit the request revision to the committed revision so tentative
	// records above it are never served.
	if r.Revision == 0 || r.Revision > committed {
		r.Revision = committed
	}

	err := commonapi.RangeStream(cs.db, rs.Context(), r, committed, rs.Send)
	if cs.metrics != nil {
		result := "success"
		if err != nil {
			result = "error"
		}

		cs.metrics.RequestsTotal.WithLabelValues("range_stream", result).Inc()
		cs.metrics.RequestDuration.WithLabelValues("range_stream").Observe(
			time.Since(start).Seconds(),
		)
	}

	return err
}
