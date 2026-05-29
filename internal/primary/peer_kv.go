// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"context"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
)

// TxnHandler processes a TxnRequest through the full local write path,
// including SQLite commit, replication, and watch distribution.
type TxnHandler func(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error)

// PeerKVServer implements the etcd KV gRPC service on the Peer API server.
// Only Txn is implemented; all other KV methods return Unimplemented. This
// allows Replicas to proxy write requests to the Primary over the existing
// peer connection without custom proto definitions.
type PeerKVServer struct {
	pb.UnimplementedKVServer
	handler TxnHandler
}

// NewPeerKVServer returns a PeerKVServer that delegates Txn writes to the
// given handler function.
func NewPeerKVServer(handler TxnHandler) *PeerKVServer {
	return &PeerKVServer{handler: handler}
}

// Txn processes a write request received from a Replica via the Peer API.
// It delegates to the handler, which runs the same local write path used
// for directly-received client requests.
func (s *PeerKVServer) Txn(ctx context.Context, r *pb.TxnRequest) (*pb.TxnResponse, error) {
	return s.handler(ctx, r)
}
