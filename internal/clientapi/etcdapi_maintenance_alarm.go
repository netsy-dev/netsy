// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"context"

	"github.com/netsy-dev/netsy/internal/nodestate"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Alarm returns the current alarm status. Netsy does not implement real etcd
// alarm semantics (NOSPACE, CORRUPT), but reports a CORRUPT alarm when the
// node's Health State is not Healthy (Loading or Degraded). This ensures
// tools like `etcdctl endpoint health` correctly reflect node health.
func (cs *ClientAPIServer) Alarm(_ context.Context, _ *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	resp := &pb.AlarmResponse{
		Header: cs.responseHeader(),
	}

	if cs.state.Health() != nodestate.HealthHealthy {
		resp.Alarms = []*pb.AlarmMember{
			{
				MemberID: cs.state.MemberID(),
				Alarm:    pb.AlarmType_CORRUPT,
			},
		}
	}

	return resp, nil
}
