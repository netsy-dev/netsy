// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"context"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/peerclient"
	"github.com/netsy-dev/netsy/internal/proto"
)

// RevisionSource provides the latest revision for node state queries.
type RevisionSource interface {
	LatestRevision() (int64, error)
}

// WatchRevisionSource provides the minimum watch revision across all
// active watches on this node. Returns -1 when no watches are active.
// It also supports setting the watch-admission floor for the compaction
// notice protocol.
type WatchRevisionSource interface {
	MinWatchRevision() int64
	SetWatchAdmissionFloor(revision int64) error
}

// Server implements the proto.NodeServer gRPC interface. It is hosted
// by every Node and called by the Elector for cluster state pushes and
// node state queries during Primary election.
type Server struct {
	proto.UnimplementedNodeServer

	logger    *slog.Logger
	nodeID    string
	startTime int64
	state     *nodestate.State
	db        RevisionSource
	peers     *peerclient.Manager
	watches   WatchRevisionSource
	guard     *ElectorGuard
}

// NewServer creates a new Node gRPC server.
func NewServer(
	logger *slog.Logger,
	nodeID string,
	startTime int64,
	state *nodestate.State,
	db RevisionSource,
	peers *peerclient.Manager,
	watches WatchRevisionSource,
) *Server {
	return &Server{
		logger:    logger,
		nodeID:    nodeID,
		startTime: startTime,
		state:     state,
		db:        db,
		peers:     peers,
		watches:   watches,
		guard:     NewElectorGuard(logger),
	}
}

// GetNodeState returns the current node state triple, latest revision,
// and start time. Called by the Elector during primary election.
func (s *Server) GetNodeState(_ context.Context, _ *emptypb.Empty) (*proto.NodeState, error) {
	latestRevision, err := s.db.LatestRevision()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get latest revision: %v", err)
	}

	return &proto.NodeState{
		NodeId:         s.nodeID,
		HealthState:    nodestate.HealthToProto(s.state.Health()),
		ElectorState:   nodestate.ElectorToProto(s.state.Elector()),
		PrimaryState:   nodestate.PrimaryToProto(s.state.Primary()),
		LatestRevision: latestRevision,
		StartTime:      s.startTime,
	}, nil
}

// PushClusterState receives and applies a cluster state update from the
// Elector. It validates the update, checks for split-brain, converts the
// proto message to the internal type, and delegates to the applier.
func (s *Server) PushClusterState(ctx context.Context, req *proto.ClusterState) (*emptypb.Empty, error) {
	if req.GetElector() == nil {
		return nil, status.Error(codes.InvalidArgument, "elector info is required")
	}

	electorNodeID := req.GetElector().GetNodeId()
	if electorNodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "elector node_id is required")
	}

	if err := s.guard.Check(electorNodeID); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "split-brain detected: %v", err)
	}

	cs := nodestate.ClusterStateFromProto(req)

	s.logger.Info("received cluster state push",
		"elector_node_id", cs.Elector.NodeID,
		"primary_node_id", cs.Primary.NodeID,
	)

	s.peers.ApplyClusterState(ctx, cs)

	return &emptypb.Empty{}, nil
}

// GetMinWatchRevision returns the minimum revision across all active
// watches on this node. If no watches are active, the current
// committed revision is returned instead, indicating that this node
// has no outstanding watchers/watches below that point.
func (s *Server) GetMinWatchRevision(_ context.Context, _ *emptypb.Empty) (*proto.MinWatchRevisionResponse, error) {
	minRev := s.watches.MinWatchRevision()
	if minRev < 0 {
		minRev = s.state.Committed()
	}

	return &proto.MinWatchRevisionResponse{
		MinRevision: minRev,
	}, nil
}

// SendCompactionNotice handles a compaction notice from the Primary.
// It atomically raises the watch-admission floor to the proposed
// compaction revision and validates that no active watch would be
// invalidated. Returns confirmed=true on success, confirmed=false
// if an active watch exists below the proposed revision.
func (s *Server) SendCompactionNotice(_ context.Context, req *proto.CompactionNotice) (*proto.CompactionNoticeResponse, error) {
	revision := req.GetCompactionRevision()

	if err := s.watches.SetWatchAdmissionFloor(revision); err != nil {
		s.logger.Warn("compaction notice rejected",
			"compaction_revision", revision,
			"reason", err,
		)
		return &proto.CompactionNoticeResponse{Confirmed: false}, nil
	}

	s.logger.Info("compaction notice accepted",
		"compaction_revision", revision,
	)
	return &proto.CompactionNoticeResponse{Confirmed: true}, nil
}
