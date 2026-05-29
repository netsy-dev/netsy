// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/netsy-dev/netsy/internal/discovery"
	"github.com/netsy-dev/netsy/internal/metrics"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/peerclient"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
)

// RevisionSource provides the latest revision for election tie-breaking.
type RevisionSource interface {
	LatestRevision() (int64, error)
}

// HeartbeatForwarder forwards heartbeats to another subsystem. When the
// node is both Elector and Primary, heartbeats received by the Elector
// are forwarded to the Primary so that its replica health tracker is
// updated using the same server-side code path.
type HeartbeatForwarder interface {
	SendHeartbeat(context.Context, *proto.NodeState) (*emptypb.Empty, error)
}

// Server implements the proto.ElectorServer gRPC interface. It is only
// active when the local node is the Elector (leader).
type Server struct {
	proto.UnimplementedElectorServer

	logger            *slog.Logger
	clusterID         string
	store             storage.ObjectStorage
	state             *nodestate.State
	nodeMap           *NodeMap
	deregTimeout      time.Duration
	heartbeatInterval time.Duration
	degradationCount  int

	// Fields for primary election.
	localNodeID         string
	localStartTime      int64
	localDB             RevisionSource
	quorum              int
	primaryPriorTimeout time.Duration
	peers               *peerclient.Manager

	// previousPrimary holds the identity of the last known Primary so
	// that checkPreviousPrimary can contact it for a drain check even
	// after the Primary has been cleared from ClusterState.
	previousPrimary atomic.Pointer[nodestate.NodeInfo]

	metrics      *Metrics
	retryMetrics *metrics.RetryMetrics

	heartbeatForwarderMu sync.RWMutex
	heartbeatForwarder   HeartbeatForwarder
}

func (s *Server) loadPreviousPrimary() nodestate.NodeInfo {
	prev := s.previousPrimary.Load()
	if prev == nil {
		return nodestate.NodeInfo{}
	}
	return *prev
}

func (s *Server) storePreviousPrimary(prev nodestate.NodeInfo) {
	s.previousPrimary.Store(&prev)
}

// NewServer creates a new Elector gRPC server.
func NewServer(
	logger *slog.Logger,
	clusterID string,
	store storage.ObjectStorage,
	state *nodestate.State,
	heartbeatInterval time.Duration,
	deregTimeout time.Duration,
	degradationCount int,
	localNodeID string,
	localStartTime int64,
	localDB RevisionSource,
	quorum int,
	primaryPriorTimeout time.Duration,
	peers *peerclient.Manager,
	m *Metrics,
	retryMetrics *metrics.RetryMetrics,
) *Server {
	return &Server{
		logger:              logger,
		clusterID:           clusterID,
		store:               store,
		state:               state,
		nodeMap:             NewNodeMap(logger.With("component", "node-map")),
		deregTimeout:        deregTimeout,
		heartbeatInterval:   heartbeatInterval,
		degradationCount:    degradationCount,
		localNodeID:         localNodeID,
		localStartTime:      localStartTime,
		localDB:             localDB,
		quorum:              quorum,
		primaryPriorTimeout: primaryPriorTimeout,
		peers:               peers,
		metrics:             m,
		retryMetrics:        retryMetrics,
	}
}

// SetHeartbeatForwarder sets or clears the forwarder that receives a copy
// of every heartbeat processed by the Elector. Pass nil to clear.
func (s *Server) SetHeartbeatForwarder(f HeartbeatForwarder) {
	s.heartbeatForwarderMu.Lock()
	s.heartbeatForwarder = f
	s.heartbeatForwarderMu.Unlock()
}

// RegisterNode registers a node with the Elector, allocating or reusing a
// member_id. It returns the assigned member_id and the current cluster state.
func (s *Server) RegisterNode(ctx context.Context, req *proto.RegisterNodeRequest) (resp *proto.RegisterNodeResponse, err error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if req.GetNodeId() == "" || req.GetClientAdvertiseAddress() == "" || req.GetPeerAdvertiseAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id, client_advertise_address, and peer_advertise_address are required")
	}
	if !s.nodeMap.Ready() {
		return nil, status.Error(codes.Unavailable, "elector is still bootstrapping")
	}

	s.logger.Debug("registering node",
		"node_id", req.GetNodeId(),
		"client_addr", req.GetClientAdvertiseAddress(),
		"peer_addr", req.GetPeerAdvertiseAddress(),
	)

	regStart := time.Now()
	memberID, err := s.allocateOrReuseMemberID(ctx, req.GetNodeId())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to allocate member_id: %v", err)
	}

	s.nodeMap.Add(NodeEntry{
		NodeID:                 req.GetNodeId(),
		MemberID:               memberID,
		ClientAdvertiseAddress: req.GetClientAdvertiseAddress(),
		PeerAdvertiseAddress:   req.GetPeerAdvertiseAddress(),
		LastHeartbeat:          time.Now(),
		HealthState:            nodestate.HealthLoading,
		PrimaryState:           nodestate.PrimaryReplica,
	})

	cs := s.state.ClusterState()
	cs.NodeCount = s.nodeMap.Count()
	protoCS := nodestate.ClusterStateToProto(cs)

	s.logger.Info("node_registered",
		"target_node_id", req.GetNodeId(),
		"member_id", memberID,
		"trigger", "direct",
		"duration_ms", time.Since(regStart).Milliseconds(),
	)

	return &proto.RegisterNodeResponse{
		MemberId:     memberID,
		ClusterState: protoCS,
	}, nil
}

// DeregisterNode removes a node from the Elector's node map and deletes
// its registration file. The durable member_id mapping in members.json is
// preserved for future re-registration.
func (s *Server) DeregisterNode(ctx context.Context, req *proto.DeregisterNodeRequest) (_ *emptypb.Empty, err error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	nodeID := req.GetNodeId()

	entry, hasMember := s.nodeMap.Get(nodeID)
	var memberID uint64
	if hasMember {
		memberID = entry.MemberID
	}

	s.logger.Info("node_deregistered",
		"target_node_id", nodeID,
		"member_id", memberID,
		"trigger", "direct",
		"reason", "shutdown",
	)

	s.nodeMap.MarkDeregistered(nodeID)
	s.nodeMap.Remove(nodeID)

	if cs := s.state.ClusterState(); cs.Primary.NodeID == nodeID {
		s.clearPrimary(ctx, "primary deregistered via RPC")
	}

	return &emptypb.Empty{}, nil
}

// GetClusterState returns the current cluster state as known by the Elector.
func (s *Server) GetClusterState(_ context.Context, _ *emptypb.Empty) (resp *proto.ClusterState, err error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if !s.nodeMap.Ready() {
		return nil, status.Error(codes.Unavailable, "elector is still bootstrapping")
	}

	cs := s.state.ClusterState()
	cs.NodeCount = s.nodeMap.Count()
	return nodestate.ClusterStateToProto(cs), nil
}

// SendHeartbeat receives a NodeState heartbeat from a Node, updating the
// node map with the latest heartbeat timestamp and reported state. When a
// HeartbeatForwarder is set (i.e. this node is also the Primary), the
// heartbeat is forwarded so the Primary's replica tracker is updated
// using the same server-side code path.
func (s *Server) SendHeartbeat(ctx context.Context, req *proto.NodeState) (_ *emptypb.Empty, err error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	health := nodestate.HealthFromProto(req.GetHealthState())
	primary := nodestate.PrimaryFromProto(req.GetPrimaryState())

	if !s.nodeMap.UpdateHeartbeat(req.GetNodeId(), time.Now(), health, primary, req.GetLatestRevision(), req.GetStartTime()) {
		return nil, status.Errorf(codes.NotFound, "node %s is not registered", req.GetNodeId())
	}

	s.heartbeatForwarderMu.RLock()
	fwd := s.heartbeatForwarder
	s.heartbeatForwarderMu.RUnlock()
	if fwd != nil && req.GetNodeId() != s.localNodeID {
		// Best-effort: the primary may not know this replica yet (e.g.
		// follow stream not established), so NotFound errors are expected.
		if _, err := fwd.SendHeartbeat(ctx, req); err != nil {
			s.logger.Debug("heartbeat forward to primary skipped", "node_id", req.GetNodeId(), "error", err)
		}
	}

	return &emptypb.Empty{}, nil
}

// requireLeader returns a gRPC error if this node is not the Elector leader.
func (s *Server) requireLeader() error {
	if s.state.Elector() != nodestate.ElectorLeader {
		return status.Error(codes.FailedPrecondition, "this node is not the elector")
	}
	return nil
}

// GetMemberList returns all registered nodes from the Elector's in-memory
// node map. Only callable when this node is the Elector leader and the
// node map bootstrap has completed.
func (s *Server) GetMemberList(_ context.Context, _ *proto.GetMemberListRequest) (*proto.GetMemberListResponse, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	if !s.nodeMap.Ready() {
		return nil, status.Error(codes.Unavailable, "elector is still bootstrapping")
	}

	entries := s.nodeMap.All()
	members := make([]*proto.MemberEntry, 0, len(entries))
	for _, e := range entries {
		members = append(members, &proto.MemberEntry{
			NodeId:                 e.NodeID,
			MemberId:               e.MemberID,
			ClientAdvertiseAddress: e.ClientAdvertiseAddress,
			PeerAdvertiseAddress:   e.PeerAdvertiseAddress,
		})
	}

	return &proto.GetMemberListResponse{Members: members}, nil
}

// allocateOrReuseMemberID reads members.json, reuses an existing member_id
// for the node if present, or allocates a new one. The updated members.json
// is written back with a conditional write and retried on precondition failure.
func (s *Server) allocateOrReuseMemberID(ctx context.Context, nodeID string) (memberID uint64, err error) {
	const maxRetries = 5
	for attempt := range maxRetries {
		mf, err := discovery.ReadMembersFile(ctx, s.store)
		if err != nil {
			return 0, fmt.Errorf("read members file: %w", err)
		}

		if id, ok := discovery.FindMemberID(mf, nodeID); ok {
			return id, nil
		}

		newID := discovery.AllocateMemberID(mf)
		mf.Members[nodeID] = newID

		if err := discovery.WriteMembersFile(ctx, s.store, mf); err != nil {
			if errors.Is(err, storage.ErrPrecondition) {
				if s.retryMetrics != nil {
					s.retryMetrics.Inc("node_registration")
				}
				s.logger.Info("members.json write conflict, retrying",
					"attempt", attempt+1,
					"node_id", nodeID,
				)
				continue
			}
			return 0, fmt.Errorf("write members file: %w", err)
		}
		return newID, nil
	}
	return 0, fmt.Errorf("failed to allocate member_id after %d retries", maxRetries)
}
