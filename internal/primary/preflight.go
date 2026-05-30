// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/nodestate"
	pb "github.com/netsy-dev/netsy/internal/proto"
)

// startPreflightLocked begins or resumes the Primary preflight loop while this
// node is in the Starting state. The caller must already hold svcMu.
func (s *Server) startPreflightLocked(parent context.Context) {
	if s.preflightCancel != nil {
		return
	}
	if s.state.Primary() != nodestate.PrimaryStarting {
		return
	}

	ctx, cancel := context.WithCancel(parent)
	s.preflightID++
	runID := s.preflightID
	s.preflightCancel = cancel

	go s.runPreflight(ctx, runID)
}

// stopPreflight cancels any in-flight Primary preflight loop.
func (s *Server) stopPreflight() {
	if s.preflightCancel == nil {
		return
	}

	s.preflightCancel()
	s.preflightCancel = nil
}

// runPreflight retries Primary preflight until it succeeds or the node is no
// longer eligible to continue in the Starting state.
func (s *Server) runPreflight(ctx context.Context, runID uint64) {
	defer func() {
		s.preflightMu.Lock()
		if s.preflightID == runID {
			s.preflightCancel = nil
		}
		s.preflightMu.Unlock()
	}()

	for {
		if s.state.Primary() != nodestate.PrimaryStarting {
			return
		}

		s.logger.Info("primary_preflight_stage_started", "stage", "preflight")
		stageStart := time.Now()
		err := s.runPreflightPass(ctx)
		switch {
		case err == nil:
			if s.metrics != nil {
				s.metrics.PreflightStageDur.WithLabelValues("preflight", "success").Observe(time.Since(stageStart).Seconds())
			}
			s.logger.Info("primary_preflight_stage_completed", "stage", "preflight", "result", "success", "duration_ms", time.Since(stageStart).Milliseconds())
			if err := s.state.SetPrimary(nodestate.PrimaryActive); err != nil {
				s.logger.Error("failed to transition primary to active after preflight", "error", err)
			}
			return
		case errors.Is(err, context.Canceled), errors.Is(err, errPreflightStopped):
			return
		default:
			if s.metrics != nil {
				s.metrics.PreflightStageDur.WithLabelValues("preflight", "error").Observe(time.Since(stageStart).Seconds())
			}
			s.logger.Warn("primary_preflight_stage_completed",
				"stage", "preflight",
				"result", "error",
				"error", err,
				"duration_ms", time.Since(stageStart).Milliseconds(),
				"retry_delay", preflightRetryDelay,
			)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(preflightRetryDelay):
		}
	}
}

// runPreflightPass performs one complete Primary preflight pass from durable
// object-storage reconciliation through committed revision activation.
func (s *Server) runPreflightPass(ctx context.Context) error {
	s.logger.Info("primary preflight started")

	latestLocalRevision, err := s.db.LatestRevision()
	if err != nil {
		return fmt.Errorf("read latest local revision before primary preflight: %w", err)
	}

	latestSnapshotInfo, err := datastore.GetLatestSnapshot(ctx, s.storageClient)
	if err != nil {
		return fmt.Errorf("get latest snapshot during primary preflight: %w", err)
	}
	if latestSnapshotInfo != nil && latestSnapshotInfo.Found && latestSnapshotInfo.Revision > latestLocalRevision {
		return fmt.Errorf("local revision %d is behind latest durable snapshot revision %d", latestLocalRevision, latestSnapshotInfo.Revision)
	}

	chunks, err := datastore.ListChunks(ctx, s.storageClient, 0)
	if err != nil {
		return fmt.Errorf("list chunks during primary preflight: %w", err)
	}

	durableRevision := int64(0)
	if latestSnapshotInfo != nil && latestSnapshotInfo.Found {
		durableRevision = latestSnapshotInfo.Revision
	}

	for _, chunk := range chunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if s.state.Primary() != nodestate.PrimaryStarting {
			return errPreflightStopped
		}

		if chunk.Revision > durableRevision {
			durableRevision = chunk.Revision
		}
		if chunk.Revision <= latestLocalRevision {
			continue
		}
		if err := datastore.DownloadAndImportFile(
			ctx,
			s.logger,
			s.db,
			s.storageClient,
			s.config.DataDir,
			chunk.Key,
			chunk.Size,
			pb.FileKind_KIND_CHUNK,
			s.storageMetrics,
		); err != nil {
			return fmt.Errorf("import chunk %s during primary preflight: %w", chunk.Key, err)
		}
	}

	if s.metrics != nil {
		s.metrics.ObjectStorageRevision.Set(float64(durableRevision))
	}

	// Insert an initial record at revision 1 if cluster is empty so the
	// committed revision starts at 1 for parity with etcd. The records-replay
	// loop below uploads it as a normal chunk.
	if durableRevision == 0 && latestLocalRevision == 0 {
		initial := &pb.Record{
			Revision: 1,
			Key:      []byte(initialRecordKey),
			Created:  true,
			Value:    []byte{},
			LeaderId: s.config.NodeID,
		}
		if _, err := s.db.InsertRecord(initial, nil); err != nil {
			return fmt.Errorf("insert initial record: %w", err)
		}
		s.logger.Info("initialized cluster", "key", initialRecordKey, "revision", initial.Revision)
	}

	records, err := s.db.FindRecordsAfterRevision(durableRevision)
	if err != nil {
		return fmt.Errorf("list local records above durable revision %d: %w", durableRevision, err)
	}

	for _, record := range records {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if s.state.Primary() != nodestate.PrimaryStarting {
			return errPreflightStopped
		}

		if err := s.writeRecordIfMissing(ctx, record); err != nil {
			return err
		}
		s.BroadcastRecord(record)
	}

	currentCompactionRevision, err := s.db.LatestCompactionRevision()
	if err != nil {
		return fmt.Errorf("read compaction revision during primary preflight: %w", err)
	}
	if currentCompactionRevision == 0 {
		currentCompactionRevision, err = s.db.DeriveCompactionRevision()
		if err != nil {
			return fmt.Errorf("derive compaction revision during primary preflight: %w", err)
		}
		if err := s.db.PersistCompactionRevision(currentCompactionRevision); err != nil {
			return fmt.Errorf("persist compaction revision during primary preflight: %w", err)
		}
	}
	s.state.SetCompaction(currentCompactionRevision)

	latestRevision, err := s.db.LatestRevision()
	if err != nil {
		return fmt.Errorf("read latest revision after preflight sync: %w", err)
	}

	s.state.SetCommitted(latestRevision)
	s.nextRevisionID.Store(latestRevision + 1)
	s.BroadcastCommit(latestRevision)

	s.logger.Info("primary preflight completed",
		"durable_revision", durableRevision,
		"committed_revision", latestRevision,
	)

	return nil
}
