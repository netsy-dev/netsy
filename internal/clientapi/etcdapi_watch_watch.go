// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package clientapi

// glossary
// watcher - watchers represents a single gRPC bidirectional stream client
//           e.g. kube-apiserver
// watch   - watches range on/track specific events. multiple per watcher.
//           e.g. multiple `kubectl watch` commands connected to a
//                single kube-apiserver watcher.

import (
	"time"

	"github.com/netsy-dev/netsy/internal/watch"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Watch is a handler for pb.Watch_WatchServer requests
// It is invoked on the creation of a new 'watcher' server, which is a gRPC
// bidirectional stream (where one kube-apiserver is the main client, though
// we may need to support multiple clients at some point).
// Note that this Watch handler is invoked on its own go routine.
// Watch loops on the gRPC stream until it receives an error, such as when
// a client disconnects or the context is cancelled.
// Watchers/clients can have multiple 'watches', and will coelesce multiple
// 'watches' on the one Watch stream e.g. a kube-apiserver will have a single
// stream but multiple 'kubectl watch' commands would be coalesced into its
// one stream.
// Each watcher has an 'inbox' channel. Watch runs a separate goroutine
// to process any incoming messages on the inbox channel and send back to
// the watcher. The inbox channel messages are expected to already be
// a WatchResponse.
func (cs *ClientAPIServer) Watch(ws pb.Watch_WatchServer) error {
	// create a new watcher and register it with the watch manager
	w := watch.NewWatcher(ws)
	watcherID := cs.watchManager.Register(w)
	if cs.metrics != nil {
		cs.metrics.Watchers.Inc()
	}

	// start a goroutine to handle messages on the inbox channel
	go func() {
		for {
			// block until next message is received
			msg, ok := <-w.InboxCh()

			// end goroutine once channel is closed
			// this will happen if Cleanup is invoked (at end of Watch method)
			if !ok {
				cs.logger.Debug("watch inbox channel closed", "watcher_id", watcherID)
				return
			}

			// send message back to client
			// w.Send serializes writes to the gRPC stream via sendMu
			if err := w.Send(&msg); err != nil {
				cs.logger.Debug("watch send failed", "watcher_id", watcherID, "error", err)
				return
			}
		}
	}()

	// we use PollUntilContextCancel to invoke progress reporting on an interval
	// it will continue until the context is cancelled or hits a deadline.
	go func() {
		_ = wait.PollUntilContextCancel(
			w.Client().Context(),
			// TODO: add jitter so we don't send updates to all watchers at the same time
			time.Second*5,
			true,
			w.ReportProgressOnInterval(cs.state.Committed, cs.logger),
		)
	}()

	// block until gRPC stream is closed
	var err error
	recvCh := make(chan *pb.WatchRequest)
	recvErrCh := make(chan error, 1)
	// Recv runs separately so server-side watcher shutdown can return from Watch
	// while Recv is blocked on the client stream. The goroutine exits when the
	// client disconnects or gRPC tears down the stream after Watch returns.
	go func() {
		for {
			// wait for next message or error from gRPC stream
			msg, recvErr := w.Client().Recv()
			if recvErr != nil {
				recvErrCh <- recvErr
				return
			}
			select {
			case recvCh <- msg:
			case <-w.Done():
				return
			}
		}
	}()
watchLoop:
	for {
		var msg *pb.WatchRequest
		// wait for the next client request, client-side stream close, or
		// server-side watcher close caused by a slow inbox.
		select {
		case msg = <-recvCh:
		case err = <-recvErrCh:
			cs.logger.Debug("watch stream closed", "watcher_id", watcherID)
			break watchLoop
		case <-w.Done():
			cs.logger.Debug("watch stream closed by server", "watcher_id", watcherID)
			break watchLoop
		}
		if msg == nil {
			continue
		}
		if cr := msg.GetCreateRequest(); cr != nil {
			// handle watch create request
			w.CreateWatch(cr, cs.state.Committed(), cs.state.Compaction(), cs.watchManager.WatchAdmissionFloor, cs.db.GetRevision, cs.logger)
			cs.updateWatchMetrics()
		}
		if cr := msg.GetCancelRequest(); cr != nil {
			// handle watch cancel request
			w.CancelWatch(cr.WatchId, cs.state.Committed(), nil, cs.logger)
			cs.updateWatchMetrics()
		}
		if pr := msg.GetProgressRequest(); pr != nil {
			// handle watch progress request
			_, _ = w.ReportProgressOnInterval(cs.state.Committed, cs.logger)(w.Client().Context())
		}
	}

	// if above loop has exited, it means the stream is closed, so cleanup
	if cs.metrics != nil {
		cs.metrics.Watchers.Dec()
	}
	w.Cleanup(cs.watchManager, cs.logger)
	cs.updateWatchMetrics()
	return err
}

// updateWatchMetrics refreshes the Watches and WatchMinRevision gauges.
func (cs *ClientAPIServer) updateWatchMetrics() {
	if cs.metrics == nil || cs.watchManager == nil {
		return
	}
	cs.metrics.Watches.Set(float64(cs.watchManager.WatchCount()))
	minRev := cs.watchManager.MinWatchRevision()
	if minRev >= 0 {
		cs.metrics.WatchMinRevision.Set(float64(minRev))
	}
}
