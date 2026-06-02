// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"context"
	"fmt"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (cs *ClientAPIServer) LeaseGrant(ctx context.Context, r *pb.LeaseGrantRequest) (resp *pb.LeaseGrantResponse, err error) {
	// TODO
	cs.logger.Warn("lease grant not implemented", "req", fmt.Sprintf("%+v", r))
	return &pb.LeaseGrantResponse{
		Header: &pb.ResponseHeader{},
		ID:     r.TTL,
		TTL:    r.TTL,
	}, nil
}

func (cs *ClientAPIServer) LeaseRevoke(ctx context.Context, r *pb.LeaseRevokeRequest) (resp *pb.LeaseRevokeResponse, err error) {
	cs.logger.Warn("LeaseRevoke not implemented")
	return nil, status.Errorf(codes.Unimplemented, "method LeaseRevoke not implemented")
}

func (cs *ClientAPIServer) LeaseKeepAlive(ka pb.Lease_LeaseKeepAliveServer) error {
	cs.logger.Warn("LeaseKeepAlive not implemented")
	return fmt.Errorf("method LeaseKeepAlive not implemented")
}

func (cs *ClientAPIServer) LeaseTimeToLive(ctx context.Context, r *pb.LeaseTimeToLiveRequest) (resp *pb.LeaseTimeToLiveResponse, err error) {
	cs.logger.Warn("LeaseTimeToLive not implemented")
	return nil, status.Errorf(codes.Unimplemented, "method LeaseTimeToLive not implemented")
}

func (cs *ClientAPIServer) LeaseLeases(ctx context.Context, r *pb.LeaseLeasesRequest) (resp *pb.LeaseLeasesResponse, err error) {
	cs.logger.Warn("LeaseLeases not implemented")
	return nil, status.Errorf(codes.Unimplemented, "method LeaseLeases not implemented")
}
