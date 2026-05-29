// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package watch

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/metadata"
)

// racyStream implements pb.Watch_WatchServer with a Send method that
// records overlapping callers. gRPC server streams do not guarantee that
// concurrent Send calls are safe, so this fake keeps the test focused on
// Netsy's contract: every watch-stream response path must go through
// Watcher.Send and share the same serialization mutex.
type racyStream struct {
	ctx      context.Context
	sendBusy atomic.Bool
	overlap  atomic.Bool
}

func (s *racyStream) Send(_ *pb.WatchResponse) error {
	if !s.sendBusy.CompareAndSwap(false, true) {
		s.overlap.Store(true)
	}
	time.Sleep(time.Microsecond)
	s.sendBusy.Store(false)
	return nil
}

func (s *racyStream) Recv() (*pb.WatchRequest, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *racyStream) SetHeader(metadata.MD) error  { return nil }
func (s *racyStream) SendHeader(metadata.MD) error { return nil }
func (s *racyStream) SetTrailer(metadata.MD)       {}
func (s *racyStream) Context() context.Context     { return s.ctx }
func (s *racyStream) SendMsg(interface{}) error    { return nil }
func (s *racyStream) RecvMsg(interface{}) error    { return nil }

func TestCreateWatchSendSerializedWithInboxDispatch(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream := &racyStream{ctx: ctx}
	w := NewWatcher(stream)

	go func() {
		for msg := range w.inboxCh {
			_ = w.Send(&msg)
		}
	}()

	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			w.RLock()
			ok := w.inboxOk
			w.RUnlock()
			if !ok {
				return
			}
			select {
			case w.inboxCh <- pb.WatchResponse{WatchId: 999}:
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.CreateWatch(
				&pb.WatchCreateRequest{Key: []byte("k")},
				10,
				0,
				func() int64 { return 0 },
				func(rev int64) (int64, bool, sql.NullString, error) {
					return rev, false, sql.NullString{}, nil
				},
				slog.Default(),
			)
		}()
	}
	wg.Wait()

	cancel()
	<-pumpDone

	w.Lock()
	w.inboxOk = false
	close(w.inboxCh)
	w.Unlock()

	if stream.overlap.Load() {
		t.Fatal("concurrent Send calls detected; Watcher.Send must serialize all writes to the gRPC stream")
	}
}
