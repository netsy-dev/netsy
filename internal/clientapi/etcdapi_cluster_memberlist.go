// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"context"

	"github.com/netsy-dev/netsy/internal/nodestate"
	internalproto "github.com/netsy-dev/netsy/internal/proto"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MemberList returns the current cluster member list. When this node is
// the Elector, the response is built directly from the local Elector's
// in-memory node map. Otherwise the request is proxied to the Elector
// via the peer connection.
func (cs *ClientAPIServer) MemberList(ctx context.Context, _ *pb.MemberListRequest) (*pb.MemberListResponse, error) {
	var resp *internalproto.GetMemberListResponse
	var err error

	if cs.state.Elector() == nodestate.ElectorLeader {
		resp, err = cs.memberLister.GetMemberList(ctx, &internalproto.GetMemberListRequest{})
	} else {
		client := cs.peerClients.ElectorClient()
		if client == nil {
			return nil, status.Error(codes.Unavailable, "no connection to elector")
		}
		resp, err = client.GetMemberList(ctx, &internalproto.GetMemberListRequest{})
	}
	if err != nil {
		return nil, err
	}

	members := make([]*pb.Member, 0, len(resp.GetMembers()))
	for _, m := range resp.GetMembers() {
		members = append(members, &pb.Member{
			ID:         m.GetMemberId(),
			Name:       m.GetNodeId(),
			ClientURLs: []string{"https://" + m.GetClientAdvertiseAddress()},
			PeerURLs:   []string{"https://" + m.GetPeerAdvertiseAddress()},
		})
	}

	return &pb.MemberListResponse{
		Header:  cs.responseHeader(),
		Members: members,
	}, nil
}
