// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

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

	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/metrics"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/peerclient"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/snapshot"
	"github.com/netsy-dev/netsy/internal/storage"
	"github.com/netsy-dev/netsy/internal/watch"
)

// Server implements the Primary domain layer and the proto.PrimaryServer
// gRPC interface. It handles transaction processing, the replication
// stream, and heartbeat collection from Replicas.
type Server struct {
	proto.UnimplementedPrimaryServer

	logger         *slog.Logger
	config         *config.Config
	db             localdb.Database
	storageClient  storage.ObjectStorage
	snapshotWorker *snapshot.Worker
	state          *nodestate.State
	peerClients    *peerclient.Manager
	watchManager   *watch.Manager
	metrics        *Metrics
	replicas       *Replicas
	followMu       sync.RWMutex
	followStreams  map[string]*followSession

	svcMu     sync.Mutex
	svcCancel context.CancelFunc

	preflightMu     sync.Mutex
	preflightCancel context.CancelFunc
	preflightID     uint64

	chunkBuffer *chunkBuffer

	heartbeatInterval    time.Duration
	degradationCount     int
	quorumReceiptTimeout time.Duration
	compactionMetrics    *metrics.CompactionMetrics
	retryMetrics         *metrics.RetryMetrics
	storageMetrics       *metrics.ObjectStorageMetrics

	lastWritePath string

	// leaderTxnGate serializes admission to the Primary write path.
	// It is a channel, rather than a mutex, so requests can stop waiting when
	// their client context is canceled.
	leaderTxnGate chan struct{}

	// nextRevisionID holds the next revision ID to assign.
	// Managed atomically to ensure thread-safe access.
	nextRevisionID atomic.Int64

	// receiptCollector is non-nil when a quorum transaction is waiting for
	// Receipts. At most one is active at a time because leaderTxnGate
	// serializes all writes. receiptCollectorMu guards the active receipt
	// collector.
	receiptCollector   *receiptCollector
	receiptCollectorMu sync.Mutex
}

// NewServer constructs the Primary server and seeds its next revision
// counter from the database.
func NewServer(
	logger *slog.Logger,
	conf *config.Config,
	db localdb.Database,
	snapshotWorker *snapshot.Worker,
	storageClient storage.ObjectStorage,
	state *nodestate.State,
	peerClients *peerclient.Manager,
	watchManager *watch.Manager,
	m *Metrics,
	heartbeatInterval time.Duration,
	degradationCount int,
	compactionMetrics *metrics.CompactionMetrics,
	retryMetrics *metrics.RetryMetrics,
	storageMetrics *metrics.ObjectStorageMetrics,
) (*Server, error) {
	s := &Server{
		logger:               logger,
		config:               conf,
		db:                   db,
		storageClient:        storageClient,
		snapshotWorker:       snapshotWorker,
		state:                state,
		peerClients:          peerClients,
		watchManager:         watchManager,
		metrics:              m,
		replicas:             NewReplicas(),
		followStreams:        make(map[string]*followSession),
		compactionMetrics:    compactionMetrics,
		retryMetrics:         retryMetrics,
		storageMetrics:       storageMetrics,
		leaderTxnGate:        make(chan struct{}, 1),
		quorumReceiptTimeout: defaultQuorumReceiptTimeout,
		chunkBuffer: newChunkBuffer(
			logger,
			state,
			storageClient,
			conf.NodeID,
			conf.Replication.ChunkBuffer.ThresholdSizeMB,
			conf.Replication.ChunkBuffer.ThresholdAgeMinutes,
			storageMetrics,
		),
		heartbeatInterval: heartbeatInterval,
		degradationCount:  degradationCount,
	}

	s.chunkBuffer.setMetrics(m)

	if err := s.initializeRevisionCounter(); err != nil {
		return nil, err
	}

	return s, nil
}

// StartServices starts Primary background services and, when the node is in
// the Starting state, begins the preflight loop that must complete before the
// Primary becomes Active. It is a no-op if services are already running.
func (s *Server) StartServices(parent context.Context) {
	s.svcMu.Lock()
	defer s.svcMu.Unlock()

	if s.svcCancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(parent)
	s.svcCancel = cancel

	go s.RunDegradationLoop(ctx)
	go s.chunkBuffer.Run(ctx)
	go s.RunCompactionScheduler(ctx)
	s.startPreflightLocked(ctx)
	s.logger.Info("primary services started")
}

// StopServices stops Primary background services and resets the follow
// hub and replica tracker. It is a no-op if services are not running.
func (s *Server) StopServices() {
	s.svcMu.Lock()
	defer s.svcMu.Unlock()

	s.stopPreflight()

	if s.svcCancel == nil {
		return
	}

	s.svcCancel()
	s.svcCancel = nil
	s.resetFollowStreams()
	s.replicas.Reset()
	s.logger.Info("primary services stopped")
}

// Replicas returns the server's Replica tracker.
func (s *Server) Replicas() *Replicas {
	return s.replicas
}

// GracefulDrain transitions the Primary to Draining, flushes the chunk buffer
// to object storage, and stops Primary services. It is the shutdown sequence
// that must be called before the normal node deregistration path when the
// shutting-down node is the Primary. Returns an error if the flush fails;
// callers should still proceed with shutdown even on error.
func (s *Server) GracefulDrain(ctx context.Context) error {
	drainStart := time.Now()
	ps := s.state.Primary()
	if ps == nodestate.PrimaryReplica {
		return nil
	}

	s.logger.Info("drain_started")

	// Transition to Draining if currently Active or Starting.
	if ps == nodestate.PrimaryActive || ps == nodestate.PrimaryStarting {
		if err := s.state.SetPrimary(nodestate.PrimaryDraining); err != nil {
			return fmt.Errorf("transition to draining: %w", err)
		}
	}

	// Flush the chunk buffer so all buffered records reach object storage.
	if err := s.chunkBuffer.flush(ctx, "draining"); err != nil {
		s.logger.Error("chunk buffer flush failed during graceful drain", "error", err)
		if s.metrics != nil {
			s.metrics.DrainDuration.WithLabelValues("error").Observe(time.Since(drainStart).Seconds())
		}
		s.logger.Info("drain_completed", "result", "error", "duration_ms", time.Since(drainStart).Milliseconds())
		return fmt.Errorf("chunk buffer flush: %w", err)
	}

	if s.metrics != nil {
		s.metrics.DrainDuration.WithLabelValues("success").Observe(time.Since(drainStart).Seconds())
	}
	s.logger.Info("drain_completed", "result", "success", "duration_ms", time.Since(drainStart).Milliseconds())
	return nil
}

// ResignLeadership transitions the Primary from Draining back to Replica,
// giving up leadership so the Elector can elect a new Primary. It is used by
// the self-degradation path after a successful drain and flush.
func (s *Server) ResignLeadership() error {
	if s.state.Primary() != nodestate.PrimaryDraining {
		return fmt.Errorf("cannot resign leadership: primary state is %s, not draining", s.state.Primary())
	}
	if err := s.state.SetPrimary(nodestate.PrimaryReplica); err != nil {
		return fmt.Errorf("transition from draining to replica: %w", err)
	}
	s.StopServices()
	s.logger.Info("primary leadership resigned")
	return nil
}

// initializeRevisionCounter sets the next revision ID based on the highest
// revision currently in the database. This should only be called on leader
// startup.
func (s *Server) initializeRevisionCounter() error {
	latestRevision, err := s.db.LatestRevision()
	if err != nil {
		return err
	}
	s.nextRevisionID.Store(latestRevision + 1)
	return nil
}

// SendHeartbeat receives a standalone heartbeat from a Replica. The
// same processing code path is used for Receipt-embedded heartbeats
// via ReplicaMap.UpdateHeartbeat.
func (s *Server) SendHeartbeat(_ context.Context, req *proto.NodeState) (_ *emptypb.Empty, err error) {
	if err := s.requirePrimary(); err != nil {
		return nil, err
	}
	if req.GetNodeId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	health := nodestate.HealthFromProto(req.GetHealthState())
	primary := nodestate.PrimaryFromProto(req.GetPrimaryState())

	if !s.replicas.UpdateHeartbeat(req.GetNodeId(), health, primary, req.GetLatestRevision()) {
		return nil, status.Errorf(codes.NotFound, "replica %s is not connected", req.GetNodeId())
	}

	return &emptypb.Empty{}, nil
}

// requirePrimary returns a gRPC error if this node is not in a Primary
// state that accepts connections (Starting, Active, or Draining).
func (s *Server) requirePrimary() error {
	ps := s.state.Primary()
	switch ps {
	case nodestate.PrimaryStarting, nodestate.PrimaryActive, nodestate.PrimaryDraining:
		return nil
	default:
		return status.Errorf(codes.FailedPrecondition, "this node is not the primary (state: %s)", ps)
	}
}

// requireActivePrimary returns a gRPC error if this node is not the active
// Primary that can accept client write traffic.
func (s *Server) requireActivePrimary() error {
	if s.state.Primary() != nodestate.PrimaryActive {
		return status.Errorf(codes.FailedPrecondition, "this node is not accepting writes (state: %s)", s.state.Primary())
	}

	return nil
}

// defaultQuorumReceiptTimeout is the default maximum time the Primary waits
// for Replica Receipts during a quorum transaction before rolling back.
const defaultQuorumReceiptTimeout = 1 * time.Second

// degradationCheckInterval is how often the degradation loop checks for
// Replicas that have missed heartbeats.
const degradationCheckInterval = 100 * time.Millisecond

// preflightRetryDelay is the wait between Primary preflight retries while the
// node remains elected and in the Starting state.
const preflightRetryDelay = time.Second

// errPreflightStopped reports that Primary preflight should stop because the
// node lost the Starting state or its context was cancelled.
var errPreflightStopped = errors.New("primary preflight stopped")

// RunDegradationLoop periodically checks all Replicas and marks any as
// Degraded if they have missed the configured number of consecutive
// heartbeats. It runs until ctx is cancelled.
func (s *Server) RunDegradationLoop(ctx context.Context) {
	if s.heartbeatInterval == 0 {
		s.logger.Warn("replication heartbeat interval is 0, degradation checking disabled")
		return
	}

	ticker := time.NewTicker(degradationCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.checkDegradation()
		}
	}
}

// checkDegradation iterates over all Replicas and marks any as Degraded
// if their last heartbeat exceeds degradationCount * heartbeatInterval.
func (s *Server) checkDegradation() {
	deadline := time.Duration(s.degradationCount) * s.heartbeatInterval
	now := time.Now().UnixNano()
	entries := s.replicas.All()

	for _, entry := range entries {
		if entry.Health() == nodestate.HealthDegraded {
			continue
		}
		lastHB := entry.LastHeartbeat.Load()
		if time.Duration(now-lastHB) < deadline {
			continue
		}

		s.logger.Warn("marking replica degraded due to missed heartbeats",
			"node_id", entry.NodeID,
			"last_heartbeat_age", time.Duration(now-lastHB).String(),
			"deadline", deadline,
		)

		entry.SetHealth(nodestate.HealthDegraded)
	}
}

// collectReceipt is called from the Follow receive loop when a Replica
// sends a Receipt. If a receipt collector is active for the receipted
// revision, the receipt is forwarded to it.
func (s *Server) collectReceipt(nodeID string, revision int64) {
	s.receiptCollectorMu.Lock()
	c := s.receiptCollector
	s.receiptCollectorMu.Unlock()

	if c == nil || c.revision != revision {
		return
	}
	c.collectReceipt(nodeID)
}

// setReceiptCollector installs a new receipt collector as the active one.
func (s *Server) setReceiptCollector(c *receiptCollector) {
	s.receiptCollectorMu.Lock()
	s.receiptCollector = c
	s.receiptCollectorMu.Unlock()
}

// clearReceiptCollector removes the active receipt collector.
func (s *Server) clearReceiptCollector() {
	s.receiptCollectorMu.Lock()
	s.receiptCollector = nil
	s.receiptCollectorMu.Unlock()
}
