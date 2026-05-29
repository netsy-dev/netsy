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

	resp, err := commonapi.Range(cs.db, ctx, r)
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
