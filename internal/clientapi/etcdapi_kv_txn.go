// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/netsy-dev/netsy/internal/localdb"
	"github.com/netsy-dev/netsy/internal/primary"
	"github.com/netsy-dev/netsy/internal/proto"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Txn handles an etcd Txn (write) request. If this node is the active
// Primary the request is processed locally; otherwise it is forwarded
// to the current Primary via the Peer API. Replica watch delivery for
// forwarded writes happens via the replication stream, not here.
func (cs *ClientAPIServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	start := time.Now()
	primaryID := cs.state.ClusterState().Primary.NodeID
	if primaryID != "" && primaryID != cs.config.NodeID {
		resp, err := cs.forwardTxn(ctx, r)
		if cs.metrics != nil {
			result := "success"
			if err != nil {
				result = "error"
			}
			cs.metrics.ProxyRequestsTotal.WithLabelValues("txn", result).Inc()
			cs.metrics.ProxyRequestDuration.WithLabelValues("txn").Observe(time.Since(start).Seconds())
		}
		return resp, err
	}
	resp, err := cs.ApplyTxn(ctx, r)
	if cs.metrics != nil {
		result := "success"
		if err != nil {
			result = "error"
		}
		cs.metrics.RequestsTotal.WithLabelValues("txn", result).Inc()
		cs.metrics.RequestDuration.WithLabelValues("txn").Observe(time.Since(start).Seconds())
	}
	return resp, err
}

// forwardTxn validates the TxnRequest locally to reject malformed
// requests early, then forwards it to the current Primary's KV service
// on the Peer API.
func (cs *ClientAPIServer) forwardTxn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	if _, err := primary.ParseTxnRequest(r); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid txn request: %v", err)
	}

	client := cs.peerClients.PrimaryKVClient()
	if client == nil {
		return nil, status.Error(codes.Unavailable, "no connection to primary")
	}

	return client.Txn(ctx, r)
}

// ApplyTxn processes a Txn write locally on the Primary. It is called
// directly for local requests and via the PeerKVServer for proxied
// requests from Replicas.
func (cs *ClientAPIServer) ApplyTxn(ctx context.Context, r *pb.TxnRequest) (resp *pb.TxnResponse, err error) {
	inserted, resp, err := cs.peerServer.LeaderTxn(ctx, r)
	// If any type of error occurs, logs and then always return well-formed error response
	if err != nil {
		if errors.Is(err, localdb.ErrCompareRevisionFailed) ||
			errors.Is(err, localdb.ErrCreateKeyExists) ||
			errors.Is(err, localdb.ErrDeleteKeyNotFound) {
			if len(r.Failure) > 0 {
				cs.logger.Debug("txn error", "txnerror", err.Error())
			} else {
				cs.logger.Info("txn error", "txnerror", err.Error())
			}
		} else if errors.Is(err, primary.ErrInvalidTxnRequest) || errors.Is(err, primary.ErrUnsupported) {
			// client request is invalid
			cs.logger.Error("txn error", "txnerror", err.Error())
			return nil, status.Error(codes.InvalidArgument, err.Error())
		} else {
			// internal failure
			cs.logger.Error("txn error", "txnerror", err.Error())
			return nil, status.Error(codes.Internal, err.Error())
		}
		// Best-effort latest revision retrieval
		// If this fails we still want to return a well formed error
		latestRevision, _ := cs.db.LatestRevision()
		resp = &pb.TxnResponse{
			Header: &pb.ResponseHeader{
				Revision: latestRevision,
			},
		}
	} else if inserted != nil && inserted.Created {
		cs.logger.Debug("txn created", "key", string(inserted.Key), "rev", inserted.Revision)
	} else if inserted != nil && inserted.Deleted {
		cs.logger.Debug("txn deleted", "key", string(inserted.Key), "rev", inserted.Revision)
	} else if inserted != nil {
		cs.logger.Debug("txn updated", "key", string(inserted.Key), "rev", inserted.Revision)
	}
	// Distribute to watchers — the record is already in memory
	// and committed_revision has been advanced by LeaderTxn.
	if inserted != nil {
		var prevRecord *proto.Record
		if !inserted.Created && inserted.PrevRevision > 0 {
			prevRecord, err = cs.db.FindRecordByRev(inserted.PrevRevision)
			if err != nil {
				cs.logger.Debug("find prev", "key", string(inserted.Key), "rev", inserted.Revision, "prev", inserted.PrevRevision, "err", err.Error())
			}
		}
		cs.watchManager.Distribute(inserted, prevRecord)
	}
	return resp, nil
}
