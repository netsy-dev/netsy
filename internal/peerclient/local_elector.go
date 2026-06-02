// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package peerclient

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/netsy-dev/netsy/internal/proto"
)

// localElectorClient adapts a LocalElectorServer into a proto.ElectorClient
// for transparent local heartbeat delivery when this node is the Elector.
type localElectorClient struct {
	srv LocalElectorServer
}

func (c *localElectorClient) SendHeartbeat(ctx context.Context, in *proto.NodeState, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return c.srv.SendHeartbeat(ctx, in)
}

func (c *localElectorClient) RegisterNode(_ context.Context, _ *proto.RegisterNodeRequest, _ ...grpc.CallOption) (*proto.RegisterNodeResponse, error) {
	return nil, fmt.Errorf("RegisterNode not supported on local elector client")
}

func (c *localElectorClient) DeregisterNode(_ context.Context, _ *proto.DeregisterNodeRequest, _ ...grpc.CallOption) (*emptypb.Empty, error) {
	return nil, fmt.Errorf("DeregisterNode not supported on local elector client")
}

func (c *localElectorClient) GetClusterState(_ context.Context, _ *emptypb.Empty, _ ...grpc.CallOption) (*proto.ClusterState, error) {
	return nil, fmt.Errorf("GetClusterState not supported on local elector client")
}

func (c *localElectorClient) GetMemberList(_ context.Context, _ *proto.GetMemberListRequest, _ ...grpc.CallOption) (*proto.GetMemberListResponse, error) {
	return nil, fmt.Errorf("GetMemberList not supported on local elector client")
}
