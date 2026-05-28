// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/netsy-dev/netsy/internal/commonapi"
	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/proto"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	googlepb "google.golang.org/protobuf/proto"
)

var ErrUnsupported = errors.New("Unsupported request - netsy only implementes the Kubernetes etcd API subet")

// ErrInvalidTxnRequest is returned when a Txn request is malformed or outside
// the supported request shapes.
var ErrInvalidTxnRequest = errors.New("invalid txn request")

// errQuorumNotMet is returned to the client when a quorum transaction
// fails to collect enough Receipts within the timeout.
var errQuorumNotMet = errors.New("quorum not met: insufficient replica receipts within timeout")

// LeaderTxn is our backend for the etcd transaction API, responsible for committing changes.
//
// It receives a pb.TxnRequest:
// https://pkg.go.dev/go.etcd.io/etcd/api/v3/etcdserverpb#TxnRequest
//
// It returns a pb.TxnResponse:
// https://pkg.go.dev/go.etcd.io/etcd/api/v3/etcdserverpb#TxnResponse
//
// The Kubernetes etcd client only uses a subset of the etcd transaction API.
//
// In the Kubernetes codebase, a storage interface implementation translates calls to a Kubernetes etcd client (in the etcd repository):
// * Create (->OptimisticPut): https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L259
// * GuaranteedUpdate (->OptimisticPut): https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L448C17-L448C33
// * Delete ->conditionalDelete(->OptimisticDelete): https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/storage/etcd3/store.go#L327
// The etcd repository contains the Kubernetes etcd client:
// * OptimisticPut: https://github.com/kubernetes/kubernetes/blob/master/vendor/go.etcd.io/etcd/client/v3/kubernetes/client.go#L83
// * OptimisticDelete: https://github.com/etcd-io/etcd/blob/main/client/v3/kubernetes/client.go#L109
//
// To summarise all Kubernetes etcd transaction request combinations:
//  1. compare, which checks if the mod_revision of the field:
//     -> for create requests: =0. meaning, there's no record or the key was deleted.
//     -> for update requests: =prev revision. meaning, it must match kubernetes known version.
//     -> for delete requests: =prev revision. meaning, it must match kubernetes known version.
//  2. 1x success, executed if compare succeeds.
//     -> create
//     -> update
//     -> delete
//  3. 0 or 1 failure, executed if compare fails:
//     -> create: can have no failure conditions, or range for existing key, returning single/first result.
//     -> update: range for existing key, returning single/first result.
//     -> delete: range for existing key, returning single/first result.
//
// Essentially the compare and failure condition for update and delete are the same, just success differs.
// Note that create and update can have a lease ID specified, which gets recorded in the success operation.
func (ps *Server) LeaderTxn(ctx context.Context, r *pb.TxnRequest) (record *proto.Record, parsed *pb.TxnResponse, err error) {
	txnStart := time.Now()
	var rangeResp *pb.RangeResponse
	var inserted *proto.Record
	if err := ps.requireActivePrimary(); err != nil {
		return nil, nil, err
	}
	// Serialize all leader transaction processing, but do not admit work whose
	// client request has already timed out while waiting behind another write.
	select {
	case ps.leaderTxnGate <- struct{}{}:
		defer func() { <-ps.leaderTxnGate }()
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
	// Validate and parse request
	record, err = ParseTxnRequest(r)
	if errors.Is(err, ErrUnsupported) {
		return nil, nil, fmt.Errorf("%w - request: %+v", err, r)
	} else if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrInvalidTxnRequest, err)
	}
	if isUnconditionalSinglePut(r) {
		// Apply etcd Put semantics: create absent keys, otherwise update the
		// latest live revision. The leader write gate serializes callers, so this
		// lookup remains stable until InsertRecord runs below.
		records, _, _, err := ps.db.FindRecordsBy("key = ?", []any{record.Key}, 0, 1, "ASC")
		if err != nil {
			return nil, nil, fmt.Errorf("resolve unconditional put: %w", err)
		}
		if len(records) == 0 || records[0].Deleted {
			record.Created = true
			record.PrevRevision = 0
		} else {
			record.Created = false
			record.PrevRevision = records[0].Revision
		}
	}
	if err := ps.resolveVersionCompareTxn(record, r); err != nil {
		return nil, nil, err
	}
	// Use the instance ID from config as the leader ID
	record.LeaderId = ps.config.NodeID
	// Assign the next revision ID. On quorum rollback the same revision is
	// reused because nextRevisionID is only incremented after successful commit.
	record.Revision = ps.nextRevisionID.Load()
	// Determine whether to use Path 1 (sync object storage) or Path 2
	// (quorum transactions) for this write.
	healthyForQuorum := ps.replicas.HealthyForQuorumCount()
	strategy := selectTxnStrategy(
		*ps.config.Replication.Quorum,
		ps.state.ClusterState().NodeCount,
		healthyForQuorum,
	)

	// Set initial metrics
	pathLabel := "sync"
	if strategy.useQuorum {
		pathLabel = "quorum"
	}
	if ps.lastWritePath != "" && ps.lastWritePath != pathLabel {
		ps.logger.Info("write_path_switched",
			"from", ps.lastWritePath,
			"to", pathLabel,
			"healthy_replicas", healthyForQuorum,
		)
	}
	ps.lastWritePath = pathLabel

	if ps.metrics != nil {
		ps.metrics.SetWritePath(pathLabel)
		ps.metrics.RequiredReceipts.Set(float64(strategy.requiredReceipts))
		ps.metrics.HealthyReplicas.Set(float64(healthyForQuorum))
		ps.metrics.ReceiptedReplicas.Set(float64(ps.replicas.ReceiptedCount()))
	}

	// Use transaction for writes
	tx, err := ps.db.BeginTx()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	// Insert record within transaction
	inserted, err = ps.db.InsertRecord(record, tx)
	if err != nil && isTxnCompareFalseError(err) && len(r.Failure) == 1 {
		_ = tx.Rollback()
		// Range on compare failure
		ps.logger.Debug("record insert error - executing failure op (range)", "error", err)
		err = nil
		rangeResp, err = commonapi.Range(ps.db, ctx, &pb.RangeRequest{
			Key: []byte(record.Key),
		}, ps.state.Committed())
		if rangeResp == nil {
			return nil, nil, fmt.Errorf("error getting range response: %w", err)
		}
		// Don't upload on compare failure, just handle the range response
	} else if err != nil {
		_ = tx.Rollback()
		return nil, nil, fmt.Errorf("error for %s: %w", record.Key, err)
	} else {
		// Record inserted successfully, commit via selected path.
		if strategy.useQuorum {
			err = ps.executeQuorumTxn(ctx, tx, inserted, strategy)
		} else {
			err = ps.executeObjectStorageTxn(ctx, tx, inserted)
		}
		if err != nil {
			if ps.metrics != nil {
				ps.metrics.WriteTransactions.WithLabelValues(pathLabel, "error").Inc()
				ps.metrics.WriteDuration.WithLabelValues(pathLabel, "error").Observe(time.Since(txnStart).Seconds())
			}
			ps.logger.Warn("write_failed",
				"path", pathLabel,
				"revision", record.Revision,
				"error", err,
			)
			return nil, nil, err
		}
	}
	if ps.metrics != nil {
		ps.metrics.WriteTransactions.WithLabelValues(pathLabel, "success").Inc()
		ps.metrics.WriteDuration.WithLabelValues(pathLabel, "success").Observe(time.Since(txnStart).Seconds())
	}
	ps.logger.Debug("write_transaction",
		"path", pathLabel,
		"revision", record.Revision,
		"healthy_replicas", healthyForQuorum,
		"required_receipts", strategy.requiredReceipts,
		"received_receipts", ps.replicas.ReceiptedCount(),
		"duration_ms", time.Since(txnStart).Milliseconds(),
	)

	resp, err := BuildTxnResponse(inserted, rangeResp)
	if err != nil {
		return nil, nil, fmt.Errorf("error building response: %w", err)
	}
	return inserted, resp, nil
}

func isTxnCompareFalseError(err error) bool {
	return errors.Is(err, localdb.ErrCompareRevisionFailed) ||
		errors.Is(err, localdb.ErrCreateKeyExists) ||
		errors.Is(err, localdb.ErrDeleteKeyNotFound)
}

// isUnconditionalSinglePut reports whether r is etcd clientv3 using
// Txn.Then(OpPut(...)).Commit(), which is what etcd benchmark txn-put
// emits when --txn-ops=1
func isUnconditionalSinglePut(r *pb.TxnRequest) bool {
	return len(r.Compare) == 0 &&
		len(r.Success) == 1 &&
		len(r.Failure) == 0 &&
		r.Success[0].GetRequestPut() != nil
}

// resolveVersionCompareTxn maps etcd VERSION comparisons to the latest
// mod_revision before InsertRecord performs its compare-and-write check.
// Kubernetes' etcd compactor uses Version(key) == n to guard a Put.
func (ps *Server) resolveVersionCompareTxn(record *proto.Record, r *pb.TxnRequest) error {
	if len(r.Compare) != 1 || r.Compare[0].Target != pb.Compare_VERSION {
		return nil
	}
	expectedVersion := r.Compare[0].GetVersion()
	if expectedVersion == 0 {
		return nil
	}

	records, _, _, err := ps.db.FindRecordsBy("key = ?", []any{record.Key}, 0, 1, "ASC")
	if err != nil {
		return fmt.Errorf("resolve version compare: %w", err)
	}
	if len(records) == 0 || records[0].Deleted || records[0].Version != expectedVersion {
		// Force InsertRecord's normal compare-failed path. That preserves the
		// existing failure-range response handling in LeaderTxn.
		record.PrevRevision = 0
		return nil
	}
	record.PrevRevision = records[0].Revision
	return nil
}

// executeObjectStorageTxn commits a transaction via synchronous object storage
// write (Path 1). It intentionally uses its own bounded context for the
// durable write instead of the caller's context: once the Primary has
// accepted a write, client cancellation only means the client stopped waiting,
// not that the commit protocol should abort. The underlying writeRecord
// invocation retries once on failure; if both attempts fail, the SQLite
// transaction is rolled back and the object storage recovery sequence begins.
func (ps *Server) executeObjectStorageTxn(ctx context.Context, tx *localdb.Tx, record *proto.Record) error {
	writeCtx, cancel := context.WithTimeout(context.Background(), objectStorageWriteTimeout)
	defer cancel()
	err := ps.writeRecord(writeCtx, record)
	if err != nil {
		_ = tx.Rollback()
		ps.startObjectStorageRecovery(record, err)
		return fmt.Errorf("object storage upload failed: %w", err)
	}
	// Commit SQLite transaction. If the commit fails after the record is
	// already durable in object storage, the state is ambiguous — transition
	// to Draining to prevent further writes.
	if err := tx.Commit(); err != nil {
		if setErr := ps.state.SetPrimary(nodestate.PrimaryDraining); setErr != nil {
			ps.logger.Error("failed to transition to draining after ambiguous commit",
				"commit_error", err,
				"transition_error", setErr,
			)
		}
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	if ps.metrics != nil {
		ps.metrics.ObjectStorageRevision.Set(float64(record.Revision))
	}
	// Increment revision counter only after successful commit
	ps.nextRevisionID.Add(1)
	// Broadcast record to replicas asynchronously (do not wait for Receipt)
	ps.BroadcastRecord(record)
	// Advance committed revision and notify replicas
	ps.state.SetCommitted(record.Revision)
	ps.state.SetLatest(record.Revision)
	ps.BroadcastCommit(record.Revision)
	// Calculate record size for snapshot tracking
	recordSize := int64(googlepb.Size(record))
	ps.checkAndCreateSnapshot(record.Revision, recordSize)
	return nil
}

// executeQuorumTxn commits a transaction via quorum Receipts from Replicas
// (Path 2). It sends the record to all connected healthy Replicas, then
// waits for the required number of durable Receipts within the timeout.
// On success the SQLite transaction is committed and the record is buffered
// for async object storage write. On failure the transaction is rolled back,
// timed-out Replicas are degraded, and a buffer flush is triggered.
func (ps *Server) executeQuorumTxn(ctx context.Context, tx *localdb.Tx, record *proto.Record, strategy txnStrategy) error {
	collector := newReceiptCollector(record.Revision, strategy.requiredReceipts)
	ps.setReceiptCollector(collector)
	defer ps.clearReceiptCollector()

	// Send record to all connected Replicas.
	ps.BroadcastRecord(record)

	// Wait for quorum receipts.
	if !collector.wait(ps.quorumReceiptTimeout) {
		// Quorum not met — rollback.
		_ = tx.Rollback()
		if ps.metrics != nil {
			ps.metrics.QuorumRollbacks.WithLabelValues("receipt_timeout").Inc()
		}
		ps.logger.Warn("quorum_rollback",
			"revision", record.Revision,
			"reason", "receipt_timeout",
			"required_receipts", strategy.requiredReceipts,
		)

		// Mark timed-out Replicas as unhealthy.
		for _, nodeID := range collector.unackedQuorumNodeIDs(ps.replicas.All()) {
			if entry, ok := ps.replicas.Get(nodeID); ok {
				ps.logger.Warn("marking replica degraded due to quorum receipt timeout",
					"node_id", nodeID,
					"revision", record.Revision,
				)
				entry.SetHealth(nodestate.HealthDegraded)
			}
		}

		// Trigger async flush of previously buffered records to object
		// storage to ensure already-committed data reaches durable storage
		// before we fall back to Path 1.
		go func() {
			flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := ps.chunkBuffer.flush(flushCtx, "rollback"); err != nil {
				ps.logger.Warn("quorum rollback buffer flush failed", "error", err)
			}
		}()

		// committed_revision is NOT advanced. nextRevisionID is NOT
		// incremented — the same revision will be reused on retry via
		// Path 1.
		return errQuorumNotMet
	}

	// Quorum met — commit SQLite transaction.
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit quorum transaction: %w", err)
	}

	// Increment revision counter
	ps.nextRevisionID.Add(1)

	// Advance committed revision before responding to the client so
	// Replicas can serve the record on read-after-write.
	ps.state.SetCommitted(record.Revision)
	ps.state.SetLatest(record.Revision)
	ps.BroadcastCommit(record.Revision)

	// Buffer record for async object storage write.
	if err := ps.chunkBuffer.bufferRecord(ctx, record); err != nil {
		ps.logger.Warn("chunk buffer failed after quorum commit",
			"revision", record.Revision,
			"error", err,
		)
	}

	// Calculate record size for snapshot tracking
	recordSize := int64(googlepb.Size(record))
	ps.checkAndCreateSnapshot(record.Revision, recordSize)
	return nil
}

// objectStorageWriteTimeout bounds the synchronous object storage write that
// makes a transaction durable. It intentionally does not use the client RPC
// context; once the Primary has admitted a write it must not demote itself just
// because the caller disconnected or timed out while the durable write was in
// progress.
const objectStorageWriteTimeout = 10 * time.Second

// objectStorageRecoveryMaxBackoff caps the exponential backoff for object
// storage recovery retries.
const objectStorageRecoveryMaxBackoff = 30 * time.Second

// startObjectStorageRecovery transitions the Primary to Draining and retries
// the failed object storage upload with exponential backoff in a background
// goroutine. On success the Primary resigns leadership and restarts as
// a Replica, allowing a fresh election.
func (ps *Server) startObjectStorageRecovery(record *proto.Record, cause error) {
	ps.logger.Error("object storage upload failed after retry, starting recovery",
		"revision", record.Revision,
		"error", cause,
	)
	if ps.state.Primary() == nodestate.PrimaryActive {
		if err := ps.state.SetPrimary(nodestate.PrimaryDraining); err != nil {
			ps.logger.Error("failed to transition to draining for object storage recovery",
				"error", err,
			)
		}
	}

	go func() {
		backoff := time.Second
		for {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := ps.writeRecordIfMissing(ctx, record)
			cancel()

			if err == nil {
				ps.logger.Info("object storage recovery upload succeeded, resigning leadership",
					"revision", record.Revision,
				)
				// Transition Draining -> Replica to give up leadership.
				if err := ps.state.SetPrimary(nodestate.PrimaryReplica); err != nil {
					ps.logger.Error("failed to transition to replica after recovery",
						"error", err,
					)
				}
				return
			}

			ps.logger.Warn("object storage recovery upload failed, retrying",
				"revision", record.Revision,
				"backoff", backoff,
				"error", err,
			)

			time.Sleep(backoff)
			if backoff < objectStorageRecoveryMaxBackoff {
				backoff *= 2
				if backoff > objectStorageRecoveryMaxBackoff {
					backoff = objectStorageRecoveryMaxBackoff
				}
			}
		}
	}()
}

// ParseTxnRequest validates a pb.TxnRequest and creates a proto.Record
func ParseTxnRequest(r *pb.TxnRequest) (*proto.Record, error) {
	// Special-case unconditional single-Put transactions, which is how clientv3 can
	// represent a plain Put. Kubernetes does not use this, but etcd benchmark
	// with txn-put option does.
	if isUnconditionalSinglePut(r) {
		put := r.Success[0].GetRequestPut()
		if put.PrevKv {
			return nil, fmt.Errorf("invalid request - prevKv not supported for success put operations")
		}
		return &proto.Record{
			Key:     put.Key,
			Value:   put.Value,
			Lease:   put.Lease,
			Created: true,
			Deleted: false,
		}, nil
	}

	// Validate request
	if len(r.Compare) != 1 ||
		len(r.Success) != 1 ||
		(len(r.Failure) != 0 && len(r.Failure) != 1) ||
		(r.Compare[0].Target != pb.Compare_MOD && r.Compare[0].Target != pb.Compare_VERSION) ||
		r.Compare[0].Result != pb.Compare_EQUAL {
		return nil, fmt.Errorf("invalid request - missing required fields")
	}
	compareKey := r.Compare[0].GetKey()
	compareRevision := r.Compare[0].GetModRevision()
	if r.Compare[0].Target == pb.Compare_VERSION {
		compareRevision = r.Compare[0].GetVersion()
	}
	successPut := r.Success[0].GetRequestPut()
	if successPut != nil && successPut.PrevKv {
		return nil, fmt.Errorf("invalid request - prevKv not supported for success put operations")
	}
	successDelete := r.Success[0].GetRequestDeleteRange()
	if successDelete != nil && successDelete.PrevKv {
		return nil, fmt.Errorf("invalid request - prevKv not supported for success delete operations")
	}
	if (successPut != nil && !bytes.Equal(compareKey, successPut.Key)) ||
		(successDelete != nil && !bytes.Equal(compareKey, successDelete.Key)) {
		return nil, fmt.Errorf("invalid request - key mismatch between compare and success operations")
	}
	var failureRange *pb.RangeRequest = nil
	if len(r.Failure) == 1 {
		failureRange = r.Failure[0].GetRequestRange()
		if failureRange == nil {
			return nil, fmt.Errorf("invalid request - failure operation must contain a range request")
		}
		if failureRange.RangeEnd != nil {
			return nil, fmt.Errorf("invalid request - rangeEnd not supported for failure range operations")
		}
		if !bytes.Equal(compareKey, failureRange.Key) {
			return nil, fmt.Errorf("invalid request - key mismatch between compare and failure operations")
		}
	}
	// check if create, update, or delete
	var record *proto.Record
	if compareRevision == 0 && successPut != nil && successDelete == nil {
		// create
		record = &proto.Record{
			Key:     successPut.Key,
			Value:   successPut.Value,
			Lease:   successPut.Lease,
			Created: true, // true=created
			Deleted: false,
		}
	} else if compareRevision > 0 && successPut != nil && successDelete == nil && failureRange != nil {
		// update
		record = &proto.Record{
			Key:          successPut.Key,
			Value:        successPut.Value,
			Lease:        successPut.Lease,
			Created:      false, // false=updated
			Deleted:      false,
			PrevRevision: compareRevision,
		}
	} else if compareRevision > 0 && successPut == nil && successDelete != nil && failureRange != nil {
		// delete
		record = &proto.Record{
			Key:          successDelete.Key,
			Value:        nil,
			Created:      false,
			Deleted:      true, // true=deleted
			PrevRevision: compareRevision,
		}
	} else {
		// unknown
		return nil, ErrUnsupported
	}
	return record, nil
}

// BuildTxnResponse converts a proto.Record or pb.RangeResponse to a pb.TxnResponse
func BuildTxnResponse(record *proto.Record, rangeResp *pb.RangeResponse) (*pb.TxnResponse, error) {
	response := &pb.TxnResponse{
		Header: &pb.ResponseHeader{},
	}

	if rangeResp != nil {
		// Failed Comparison - return Failure operation ResponseRange
		response.Header.Revision = rangeResp.Header.Revision
		response.Succeeded = false
		response.Responses = []*pb.ResponseOp{
			{
				Response: &pb.ResponseOp_ResponseRange{
					ResponseRange: rangeResp,
				},
			},
		}
	} else if record != nil && record.Deleted {
		// Delete operation - return DeleteRangeResponse
		response.Header.Revision = record.Revision
		response.Succeeded = true
		response.Responses = []*pb.ResponseOp{
			{
				Response: &pb.ResponseOp_ResponseDeleteRange{
					ResponseDeleteRange: &pb.DeleteRangeResponse{
						Header:  &pb.ResponseHeader{Revision: record.Revision},
						Deleted: 1,
					},
				},
			},
		}
	} else if record != nil {
		// Create or Update operation - return PutResponse
		response.Header.Revision = record.Revision
		response.Succeeded = true
		response.Responses = []*pb.ResponseOp{
			{
				Response: &pb.ResponseOp_ResponsePut{
					ResponsePut: &pb.PutResponse{
						Header: &pb.ResponseHeader{Revision: record.Revision},
					},
				},
			},
		}
	}
	return response, nil
}
