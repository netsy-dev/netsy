// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package peerclient

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/proto"
)

// PrimaryChangeFunc is called when this node's Primary role changes as
// a result of a cluster state update. isPrimary is true when this node
// has been elected Primary, and false when it has transitioned back
// to Replica.
type PrimaryChangeFunc func(isPrimary bool)

// LocalElectorServer is the subset of proto.ElectorServer needed to
// serve heartbeats locally when this node is the Elector.
type LocalElectorServer interface {
	SendHeartbeat(context.Context, *proto.NodeState) (*emptypb.Empty, error)
}

// Manager owns the outbound gRPC connections to the current Elector and
// Primary. It updates connections when the cluster state changes.
type Manager struct {
	logger    *slog.Logger
	nodeID    string
	tlsConfig *tls.Config
	state     *nodestate.State

	onPrimaryChange PrimaryChangeFunc
	localElector    LocalElectorServer

	// applyMu serializes ApplyClusterState's compound role transition.
	// onPrimaryChange runs under this lock and must not re-enter
	// ApplyClusterState on the same Manager.
	applyMu       sync.Mutex
	mu            sync.Mutex
	electorAddr   string
	electorConn   *grpc.ClientConn
	electorClient proto.ElectorClient
	primaryAddr   string
	primaryConn   *grpc.ClientConn
}

// NewManager creates a new peer Manager.
func NewManager(
	logger *slog.Logger,
	nodeID string,
	tlsConfig *tls.Config,
	state *nodestate.State,
) *Manager {
	return &Manager{
		logger:    logger,
		nodeID:    nodeID,
		tlsConfig: tlsConfig,
		state:     state,
	}
}

// SetLocalElectorServer sets the local Elector server used when this
// node is the Elector. Must be called before ConnectElector.
func (m *Manager) SetLocalElectorServer(srv LocalElectorServer) {
	m.localElector = srv
}

// SetPrimaryChangeFunc sets a callback invoked when this node's Primary
// role changes. Must be called before any cluster state updates.
func (m *Manager) SetPrimaryChangeFunc(fn PrimaryChangeFunc) {
	m.onPrimaryChange = fn
}

// ApplyClusterState applies the given cluster state to the current
// node only. It executes the role-change callback, updates state
// transitions and reconnects gRPC clients to the current Elector
// and Primary.
func (m *Manager) ApplyClusterState(ctx context.Context, cs nodestate.ClusterState) {
	m.applyMu.Lock()
	defer m.applyMu.Unlock()

	old := m.state.ClusterState()
	m.state.SetClusterState(cs)

	wasPrimary := old.Primary.NodeID == m.nodeID
	isPrimary := cs.Primary.NodeID == m.nodeID

	// If this node has been elected Primary, transition to Starting.
	if isPrimary && m.state.Primary() == nodestate.PrimaryReplica {
		if err := m.state.SetPrimary(nodestate.PrimaryStarting); err != nil {
			m.logger.Error("failed to transition to primary starting", "error", err)
		}
	}

	// If this node was elected Primary (Starting) but a different node
	// has since been elected, transition back to Replica.
	if !isPrimary && m.state.Primary() == nodestate.PrimaryStarting {
		if err := m.state.SetPrimary(nodestate.PrimaryReplica); err != nil {
			m.logger.Error("failed to transition from starting back to replica", "error", err)
		}
	}

	// Update Elector connection if changed.
	if cs.Elector.PeerAdvertiseAddr != old.Elector.PeerAdvertiseAddr {
		m.updateElector(cs.Elector)
	}

	// Update Primary connection if changed.
	if cs.Primary.PeerAdvertiseAddr != old.Primary.PeerAdvertiseAddr || cs.Primary.NodeID != old.Primary.NodeID {
		m.updatePrimary(cs.Primary)
	}

	// Notify role change listeners when the Primary role changes for
	// this node (e.g. to start/stop the replication follower), or when
	// a remote Primary appears for the first time and this node is a
	// Replica that needs to start following it. Runs after connection
	// updates so the follower can use the new Primary connection.
	if m.onPrimaryChange != nil {
		remotePrimaryAppeared := old.Primary.NodeID == "" && cs.Primary.NodeID != "" && !isPrimary
		if wasPrimary != isPrimary || remotePrimaryAppeared {
			m.onPrimaryChange(isPrimary)
		}
	}
}

// ConnectElector dials the given Elector address and sets the initial
// Elector connection and heartbeat target. When the Elector is this
// node, a local adapter is used instead of a gRPC connection.
func (m *Manager) ConnectElector(electorNodeID, addr string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if electorNodeID == m.nodeID {
		m.electorAddr = addr
		m.electorClient = &localElectorClient{srv: m.localElector}
		m.logger.Info("elector is self, using local client")
		return nil
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(credentials.NewTLS(m.tlsConfig)),
	)
	if err != nil {
		return fmt.Errorf("dial elector %s: %w", addr, err)
	}

	m.electorAddr = addr
	m.electorConn = conn
	m.electorClient = proto.NewElectorClient(conn)
	m.logger.Info("elector connection established", "addr", addr)
	return nil
}

// ElectorClient returns a client for the current Elector, or nil if
// none is established. The client may be backed by a gRPC connection
// or by a local adapter when this node is the Elector.
func (m *Manager) ElectorClient() proto.ElectorClient {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.electorClient
}

// PrimaryClient returns a gRPC Primary client for the current Primary
// connection, or nil if none is established.
func (m *Manager) PrimaryClient() proto.PrimaryClient {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.primaryConn == nil {
		return nil
	}
	return proto.NewPrimaryClient(m.primaryConn)
}

// PrimaryKVClient returns an etcd KV client for the current Primary
// connection, or nil if none is established. Used by Replicas to proxy
// write requests to the Primary's Peer API.
func (m *Manager) PrimaryKVClient() pb.KVClient {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.primaryConn == nil {
		return nil
	}
	return pb.NewKVClient(m.primaryConn)
}

// Close closes all peer connections.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.electorConn != nil {
		m.electorConn.Close()
		m.electorConn = nil
	}
	if m.primaryConn != nil {
		m.primaryConn.Close()
		m.primaryConn = nil
	}
}

func (m *Manager) dialNode(addr string) (proto.NodeClient, *grpc.ClientConn, error) {
	if m.tlsConfig == nil {
		return nil, nil, fmt.Errorf("peer TLS config not set")
	}

	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(credentials.NewTLS(m.tlsConfig)),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	return proto.NewNodeClient(conn), conn, nil
}

// GetNodeState calls GetNodeState on a remote node.
func (m *Manager) GetNodeState(ctx context.Context, addr string) (*proto.NodeState, error) {
	client, conn, err := m.dialNode(addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	return client.GetNodeState(ctx, &emptypb.Empty{})
}

// GetMinWatchRevision calls GetMinWatchRevision on a remote node.
// Returns the minimum revision across all active watches on that node,
// or committed revision if the node has no active watches.
func (m *Manager) GetMinWatchRevision(ctx context.Context, addr string) (int64, error) {
	client, conn, err := m.dialNode(addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	resp, err := client.GetMinWatchRevision(ctx, &emptypb.Empty{})
	if err != nil {
		return 0, err
	}
	return resp.GetMinRevision(), nil
}

// SendCompactionNotice sends a compaction notice to a remote node and
// returns whether the node confirmed the proposed compaction revision.
func (m *Manager) SendCompactionNotice(ctx context.Context, addr string, revision int64) (bool, error) {
	client, conn, err := m.dialNode(addr)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	resp, err := client.SendCompactionNotice(ctx, &proto.CompactionNotice{
		CompactionRevision: revision,
	})
	if err != nil {
		return false, err
	}
	return resp.GetConfirmed(), nil
}

// PushClusterStateTo pushes the given cluster state to a remote node.
func (m *Manager) PushClusterStateTo(ctx context.Context, addr string, cs *proto.ClusterState) error {
	client, conn, err := m.dialNode(addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = client.PushClusterState(ctx, cs)
	return err
}

// updateElector replaces the outbound gRPC connection to the Elector
// when the Elector's advertise address changes.
func (m *Manager) updateElector(elector nodestate.NodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if elector.PeerAdvertiseAddr == m.electorAddr {
		return
	}

	if m.electorConn != nil {
		m.electorConn.Close()
		m.electorConn = nil
	}
	m.electorClient = nil

	if elector.NodeID == m.nodeID {
		m.electorAddr = elector.PeerAdvertiseAddr
		m.electorClient = &localElectorClient{srv: m.localElector}
		m.logger.Info("elector is self, using local client")
		return
	}

	if elector.PeerAdvertiseAddr == "" {
		m.electorAddr = ""
		return
	}

	conn, err := grpc.NewClient(
		elector.PeerAdvertiseAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(m.tlsConfig)),
	)
	if err != nil {
		m.logger.Error("failed to dial elector", "addr", elector.PeerAdvertiseAddr, "error", err)
		return
	}

	m.electorAddr = elector.PeerAdvertiseAddr
	m.electorConn = conn
	m.electorClient = proto.NewElectorClient(conn)
	m.logger.Info("elector connection updated", "addr", elector.PeerAdvertiseAddr)
}

// updatePrimary replaces the outbound gRPC connection to the Primary
// when the Primary's advertise address or node ID changes.
func (m *Manager) updatePrimary(primary nodestate.NodeInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if primary.PeerAdvertiseAddr == m.primaryAddr && primary.NodeID != "" {
		return
	}

	if m.primaryConn != nil {
		m.primaryConn.Close()
		m.primaryConn = nil
	}

	if primary.NodeID == m.nodeID {
		m.primaryAddr = primary.PeerAdvertiseAddr
		m.logger.Info("primary is self, skipping outbound connection")
		return
	}

	if primary.PeerAdvertiseAddr == "" || primary.NodeID == "" {
		m.primaryAddr = ""
		return
	}

	conn, err := grpc.NewClient(
		primary.PeerAdvertiseAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(m.tlsConfig)),
	)
	if err != nil {
		m.logger.Error("failed to dial primary", "addr", primary.PeerAdvertiseAddr, "error", err)
		return
	}

	m.primaryAddr = primary.PeerAdvertiseAddr
	m.primaryConn = conn
	m.logger.Info("primary connection updated",
		"node_id", primary.NodeID,
		"addr", primary.PeerAdvertiseAddr,
	)
}
