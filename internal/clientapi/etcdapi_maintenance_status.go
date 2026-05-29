// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"context"
	"fmt"

	"github.com/netsy-dev/netsy/internal/buildvars"
	"github.com/netsy-dev/netsy/internal/nodestate"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Status returns the status of this node. Unlike MemberList, Status is
// always answered locally — it is never proxied — because the response
// reflects the responding Node's own database size, health, and view of
// the current Primary (leader).
//
// Raft-related fields (RaftIndex, RaftTerm, RaftAppliedIndex) are
// intentionally kept at zero because Netsy does not use Raft. IsLearner
// is always false because Netsy has no learner role.
func (cs *ClientAPIServer) Status(_ context.Context, _ *pb.StatusRequest) (*pb.StatusResponse, error) {
	dbSize, err := cs.db.Size()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting db size: %s", err)
	}

	dbSizeInUse, err := cs.db.SizeInUse()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting db size in use: %s", err)
	}

	// leader is the current Primary's stable etcd member_id, or 0 if
	// no Primary is known.
	cs2 := cs.state.ClusterState()
	var leader uint64
	if cs2.Primary.NodeID != "" {
		leader = cs2.Primary.MemberID
	}

	// errors reflects the responding node's local Health State.
	var errors []string
	health := cs.state.Health()
	if health != nodestate.HealthHealthy {
		errors = append(errors, fmt.Sprintf("health state: %s", health))
	}

	return &pb.StatusResponse{
		Header:      cs.responseHeader(),
		Version:     buildvars.EtcdCompatVersion,
		DbSize:      dbSize,
		DbSizeInUse: dbSizeInUse,
		Leader:      leader,
		Errors:      errors,
		// Netsy does not use Raft; these fields are intentionally static.
		RaftIndex:        0,
		RaftTerm:         0,
		RaftAppliedIndex: 0,
		// Netsy has no learner role.
		IsLearner: false,
	}, nil
}
