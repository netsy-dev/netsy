// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/netsy-dev/netsy/internal/config"
	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/discovery"
	"github.com/netsy-dev/netsy/internal/elector"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/metrics"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/peerclient"
	"github.com/netsy-dev/netsy/internal/proto"
	"github.com/netsy-dev/netsy/internal/storage"
)

// maxIntegrityAttempts caps bootstrap replay retries after an integrity
// check failure. The second attempt starts from an empty local database.
const maxIntegrityAttempts = 2

// Result captures bootstrap metadata that later startup stages can reuse
// without repeating discovery work.
type Result struct {
	LatestSnapshotInfo *datastore.LatestSnapshotInfo
}

// Follower is the subset of replication follower behaviour required during bootstrap.
type Follower interface {
	RequireInitialSync()
	Start(parent context.Context) error
	Stop()
}

// Run executes the node loading and backfill sequence, including node
// registration, initial cluster-state discovery, optional Primary follow
// stream establishment, object-storage replay, and final integrity checks.
// On success it transitions the node from Loading to Healthy and returns
// bootstrap metadata for later startup stages.
func Run(
	ctx context.Context,
	logger *slog.Logger,
	cfg *config.Config,
	state *nodestate.State,
	db localdb.Database,
	store storage.ObjectStorage,
	peers *peerclient.Manager,
	election *elector.Runner,
	follower Follower,
	metrics *Metrics,
	storageMetrics *metrics.ObjectStorageMetrics,
) (*Result, error) {
	var err error

	logger.Info("loading_stage_started", "stage", "registration")

	// ensure node registration file exists in object storage
	err = discovery.WriteNodeRegistration(ctx, store, discovery.NodeRegistration{
		NodeID:                 cfg.NodeID,
		ClientAdvertiseAddress: cfg.AdvertiseClient,
		PeerAdvertiseAddress:   cfg.AdvertisePeer,
	})
	if err != nil {
		return nil, fmt.Errorf("write node registration: %w", err)
	}

	// register with Elector (or self, if Elector)
	registerReq := &proto.RegisterNodeRequest{
		NodeId:                 cfg.NodeID,
		ClientAdvertiseAddress: cfg.AdvertiseClient,
		PeerAdvertiseAddress:   cfg.AdvertisePeer,
	}
	var registerResp *proto.RegisterNodeResponse
	regStart := time.Now()
	if election.IsLeader() {
		registerResp, err = election.RegisterLocalNode(ctx, registerReq)
		if err != nil {
			if metrics != nil {
				metrics.RegistrationDuration.WithLabelValues("error").Observe(time.Since(regStart).Seconds())
			}
			return nil, fmt.Errorf("register with local elector: %w", err)
		}
	} else {
		const maxRegistrationAttempts = 3
		backoff := 500 * time.Millisecond
		for attempt := 1; ; attempt++ {
			client := peers.ElectorClient()
			if client == nil {
				if attempt >= maxRegistrationAttempts {
					return nil, fmt.Errorf("elector client is not connected")
				}
			} else {
				registerResp, err = client.RegisterNode(ctx, registerReq)
				if err == nil {
					break
				}
				if attempt >= maxRegistrationAttempts {
					if metrics != nil {
						metrics.RegistrationDuration.WithLabelValues("error").Observe(time.Since(regStart).Seconds())
					}
					return nil, fmt.Errorf("register with elector: %w", err)
				}
			}
			logger.Info("elector not ready, retrying registration",
				"attempt", attempt,
				"backoff", backoff,
				"error", err,
			)
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("register with elector: %w", ctx.Err())
			case <-time.After(backoff):
			}
			if backoff < 5*time.Second {
				backoff *= 2
			}
		}
	}
	if metrics != nil {
		metrics.RegistrationDuration.WithLabelValues("success").Observe(time.Since(regStart).Seconds())
		metrics.LoadingStageDuration.WithLabelValues("registration", "success").Observe(time.Since(regStart).Seconds())
	}
	logger.Info("node_registered",
		"target_node_id", cfg.NodeID,
		"member_id", registerResp.GetMemberId(),
		"trigger", "startup",
		"duration_ms", time.Since(regStart).Milliseconds(),
	)
	logger.Info("loading_stage_completed", "stage", "registration", "result", "success", "duration_ms", time.Since(regStart).Milliseconds())

	// update member id from registration response into node state
	err = state.SetMemberID(registerResp.GetMemberId())
	if err != nil {
		return nil, fmt.Errorf("cache local member_id: %w", err)
	}

	// sync cluster state info into node state
	clusterState := registerResp.GetClusterState()
	if clusterState == nil || clusterState.GetElector() == nil {
		if election.IsLeader() {
			clusterState, err = election.GetLocalClusterState(ctx)
			if err != nil {
				return nil, fmt.Errorf("get local cluster state: %w", err)
			}
		} else {
			client := peers.ElectorClient()
			if client == nil {
				return nil, fmt.Errorf("elector client is not connected")
			}
			clusterState, err = client.GetClusterState(ctx, &emptypb.Empty{})
			if err != nil {
				return nil, fmt.Errorf("get cluster state from elector: %w", err)
			}
		}
	}
	peers.ApplyClusterState(ctx, nodestate.ClusterStateFromProto(clusterState))

	// get latest snapshot
	latestSnapshotInfo, err := datastore.GetLatestSnapshot(ctx, store)
	if err != nil {
		return nil, fmt.Errorf("get latest snapshot: %w", err)
	}

	// backfill local db
	backfillStart := time.Now()
	logger.Info("loading_stage_started", "stage", "backfill")
	for attempt := 1; attempt <= maxIntegrityAttempts; attempt++ {
		if attempt > 1 {
			logger.Info("loading_restarted", "reason", "integrity_check_failed", "attempt", attempt)
			if metrics != nil {
				metrics.LoadingRestarts.WithLabelValues("integrity_check_failed").Inc()
				metrics.LocalDBRebuilds.WithLabelValues("integrity_check_failed").Inc()
			}
			logger.Info("local_db_rebuild_started", "reason", "integrity_check_failed", "attempt", attempt)
			follower.Stop()
			if err := db.Truncate(); err != nil {
				return nil, fmt.Errorf("truncate local database: %w", err)
			}
			state.SetCommitted(0)
			state.SetCompaction(0)
			logger.Info("local_db_rebuild_completed", "reason", "integrity_check_failed", "attempt", attempt)
		}

		backfillFrom, err := prepareLocalState(ctx, logger, cfg, state, db, store, latestSnapshotInfo, storageMetrics)
		if err != nil {
			return nil, err
		}

		if needsFollowStream(state, cfg.NodeID) {
			follower.RequireInitialSync()
			if err := follower.Start(ctx); err != nil {
				return nil, fmt.Errorf("establish follow stream: %w", err)
			}
			if err := seedCompactionFromPrimary(state, db); err != nil {
				return nil, err
			}
		} else {
			follower.Stop()
		}

		if err := backfillChunksFromRevision(ctx, logger, db, cfg, backfillFrom, store, storageMetrics); err != nil {
			return nil, fmt.Errorf("backfill chunks: %w", err)
		}

		if err := db.VerifyIntegrity(); err != nil {
			if attempt == maxIntegrityAttempts {
				return nil, fmt.Errorf("verify integrity after backfill: %w", err)
			}
			continue
		}

		break
	}

	if !needsFollowStream(state, cfg.NodeID) {
		latestRevision, err := db.LatestRevision()
		if err != nil {
			return nil, fmt.Errorf("get latest revision after bootstrap: %w", err)
		}
		state.SetCommitted(latestRevision)
	}

	if metrics != nil {
		metrics.LoadingStageDuration.WithLabelValues("backfill", "success").Observe(time.Since(backfillStart).Seconds())
	}
	logger.Info("loading_stage_completed", "stage", "backfill", "result", "success", "duration_ms", time.Since(backfillStart).Milliseconds())

	if err := state.SetHealth(nodestate.HealthHealthy); err != nil {
		return nil, fmt.Errorf("transition to healthy: %w", err)
	}

	return &Result{LatestSnapshotInfo: latestSnapshotInfo}, nil
}

// prepareLocalState seeds durable compaction state, imports the latest
// snapshot into an empty database when present, and returns the first revision
// above the durable local baseline that still needs chunk backfill.
func prepareLocalState(
	ctx context.Context,
	logger *slog.Logger,
	cfg *config.Config,
	state *nodestate.State,
	db localdb.Database,
	store storage.ObjectStorage,
	latestSnapshotInfo *datastore.LatestSnapshotInfo,
	storageMetrics *metrics.ObjectStorageMetrics,
) (int64, error) {
	if err := seedCompactionFromLocalDB(state, db); err != nil {
		return 0, err
	}

	recordCount, err := db.RecordCount()
	if err != nil {
		return 0, fmt.Errorf("count local records: %w", err)
	}

	if recordCount > 0 {
		latestRevision, err := db.LatestRevision()
		if err != nil {
			return 0, fmt.Errorf("get latest local revision: %w", err)
		}
		return latestRevision, nil
	}

	if err := importLatestSnapshot(ctx, logger, db, cfg, latestSnapshotInfo, store, storageMetrics); err != nil {
		return 0, fmt.Errorf("import latest snapshot: %w", err)
	}

	if err := seedCompactionFromLocalDB(state, db); err != nil {
		return 0, err
	}

	if latestSnapshotInfo != nil && latestSnapshotInfo.Found {
		return latestSnapshotInfo.Revision, nil
	}

	return 0, nil
}

// seedCompactionFromLocalDB restores the persisted compaction revision, or
// derives and persists it from contiguous compacted records when the durable
// table is still empty.
func seedCompactionFromLocalDB(state *nodestate.State, db localdb.Database) error {
	current, err := db.LatestCompactionRevision()
	if err != nil {
		return fmt.Errorf("read local compaction revision: %w", err)
	}
	if current > 0 {
		state.SetCompaction(current)
		return nil
	}

	derived, err := db.DeriveCompactionRevision()
	if err != nil {
		return fmt.Errorf("derive local compaction revision: %w", err)
	}
	if err := db.PersistCompactionRevision(derived); err != nil {
		return err
	}
	state.SetCompaction(derived)
	return nil
}

// seedCompactionFromPrimary persists the compaction revision learned from the
// Primary's Initial message when local bootstrap state did not already have
// one recorded.
func seedCompactionFromPrimary(state *nodestate.State, db localdb.Database) error {
	current, err := db.LatestCompactionRevision()
	if err != nil {
		return fmt.Errorf("read local compaction revision after follow initial: %w", err)
	}
	if current > 0 || state.Compaction() == 0 {
		return nil
	}
	if err := db.PersistCompactionRevision(state.Compaction()); err != nil {
		return err
	}
	return nil
}

// needsFollowStream reports whether bootstrap should connect to a remote
// Primary instead of relying on local object storage only.
func needsFollowStream(state *nodestate.State, nodeID string) bool {
	cs := state.ClusterState()
	return cs.Primary.NodeID != "" && cs.Primary.NodeID != nodeID
}
