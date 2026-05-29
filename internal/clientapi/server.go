// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"context"
	"log/slog"

	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/peerclient"
	"github.com/netsy-dev/netsy/internal/primary"
	internalproto "github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/watch"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

// memberLister returns the cluster member list from the Elector's in-memory
// node map. Satisfied by *elector.Server when the local node is the Elector.
type memberLister interface {
	GetMemberList(context.Context, *internalproto.GetMemberListRequest) (*internalproto.GetMemberListResponse, error)
}

// ClientAPIServer implements a gRPC server compatible with the Kubernetes etcd API subset
// @see https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.proto#L37
// @see https://github.com/etcd-io/etcd/blob/main/api/etcdserverpb/rpc.pb.go
// etcd has the following gRPC "services":
// * KV
// * Watch
// * Lease
// * Cluster
// * Maintenance
// * Auth
// we include the 'Unimplemented' services by default and override them where required
type ClientAPIServer struct {
	logger       *slog.Logger
	config       *config.Config
	db           localdb.Database
	grpcServer   *grpc.Server
	state        *nodestate.State
	peerServer   *primary.Server
	peerClients  *peerclient.Manager
	watchManager *watch.Manager
	memberLister memberLister
	metrics      *Metrics

	pb.UnimplementedKVServer
	pb.UnimplementedWatchServer
	pb.UnimplementedLeaseServer
	pb.UnimplementedClusterServer
	pb.UnimplementedMaintenanceServer
	pb.UnimplementedAuthServer
}

// NewServer registers the etcd-compatible Client API services on the provided gRPC server.
// The memberLister is called locally when this node is the Elector; non-Elector
// nodes proxy MemberList requests to the Elector via peerClients.
func NewServer(logger *slog.Logger, conf *config.Config, db localdb.Database, grpcServer *grpc.Server, peerServer *primary.Server, peerClients *peerclient.Manager, watchManager *watch.Manager, state *nodestate.State, ml memberLister, m *Metrics) *ClientAPIServer {
	clientServer := &ClientAPIServer{
		logger:       logger,
		config:       conf,
		grpcServer:   grpcServer,
		db:           db,
		state:        state,
		peerServer:   peerServer,
		peerClients:  peerClients,
		watchManager: watchManager,
		memberLister: ml,
		metrics:      m,
	}

	pb.RegisterKVServer(grpcServer, clientServer)
	pb.RegisterWatchServer(grpcServer, clientServer)
	pb.RegisterLeaseServer(grpcServer, clientServer)
	pb.RegisterClusterServer(grpcServer, clientServer)
	pb.RegisterMaintenanceServer(grpcServer, clientServer)
	pb.RegisterAuthServer(grpcServer, clientServer)
	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpcServer, hsrv)
	reflection.Register(grpcServer)

	return clientServer
}

// Close gracefully stops the gRPC server and closes the database.
func (clientServer *ClientAPIServer) Close() {
	clientServer.grpcServer.GracefulStop()
	clientServer.db.Close()
}

// responseHeader returns a populated etcd ResponseHeader containing the
// stable cluster ID (FNV-1a hash) and this node's stable member ID.
func (cs *ClientAPIServer) responseHeader() *pb.ResponseHeader {
	return &pb.ResponseHeader{
		ClusterId: cs.config.EtcdClusterID,
		MemberId:  cs.state.MemberID(),
	}
}
