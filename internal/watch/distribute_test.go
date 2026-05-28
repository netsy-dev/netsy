// SPDX-License-Identifier: Apache-2.0

package watch

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/proto"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"google.golang.org/grpc/metadata"
)

type closedWatcherStream struct {
	ctx context.Context
}

func (s closedWatcherStream) Send(*pb.WatchResponse) error { return nil }
func (s closedWatcherStream) Recv() (*pb.WatchRequest, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}
func (s closedWatcherStream) SetHeader(metadata.MD) error  { return nil }
func (s closedWatcherStream) SendHeader(metadata.MD) error { return nil }
func (s closedWatcherStream) SetTrailer(metadata.MD)       {}
func (s closedWatcherStream) Context() context.Context     { return s.ctx }
func (s closedWatcherStream) SendMsg(interface{}) error    { return nil }
func (s closedWatcherStream) RecvMsg(interface{}) error    { return nil }

func TestDistributeDeliversToHealthyWatcher(t *testing.T) {
	t.Parallel()

	manager := NewManager(slog.Default(), nil)
	watcher := NewWatcher(closedWatcherStream{ctx: context.Background()})
	watcher.watches = map[int64]watchEntry{
		10: {key: []byte("key"), cancel: func() {}},
	}
	manager.Register(watcher)

	manager.Distribute(&proto.Record{Revision: 1, Key: []byte("key")}, nil)

	select {
	case msg := <-watcher.inboxCh:
		if msg.WatchId != 10 {
			t.Fatalf("WatchId = %d, want 10", msg.WatchId)
		}
		if len(msg.Events) != 1 {
			t.Fatalf("len(Events) = %d, want 1", len(msg.Events))
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for watch response")
	}
}

// TestDistributeClosesSlowWatcherWithFullInbox verifies that the write path
// does not block behind a watcher that is no longer draining its inbox.
// Distribute is called synchronously after commits, so a blocking inbox send
// would stop all later writes. A full inbox closes the slow watcher instead.
func TestDistributeClosesSlowWatcherWithFullInbox(t *testing.T) {
	t.Parallel()

	manager := NewManager(slog.Default(), nil)
	watcher := NewWatcher(closedWatcherStream{ctx: context.Background()})
	watcher.inboxCh = make(chan pb.WatchResponse, 1)
	watcher.watches = map[int64]watchEntry{
		10: {key: []byte("key"), cancel: func() {}},
	}
	manager.Register(watcher)

	watcher.inboxCh <- pb.WatchResponse{}

	distributeDone := make(chan struct{})
	go func() {
		manager.Distribute(&proto.Record{Revision: 1, Key: []byte("key")}, nil)
		close(distributeDone)
	}()

	select {
	case <-distributeDone:
	case <-time.After(time.Second):
		t.Fatal("Distribute() blocked on a full watcher inbox")
	}

	if got := manager.WatchCount(); got != 0 {
		t.Fatalf("WatchCount() = %d, want 0 after closing slow watcher", got)
	}

	select {
	case <-watcher.Done():
	case <-time.After(time.Second):
		t.Fatal("slow watcher was not signaled closed")
	}

	cleanupDone := make(chan struct{})
	go func() {
		watcher.Cleanup(manager, slog.Default())
		close(cleanupDone)
	}()

	select {
	case <-cleanupDone:
	case <-time.After(time.Second):
		t.Fatal("Cleanup() blocked after slow watcher was closed")
	}
}

func TestCreateWatchReturnsWhenWatcherInboxClosed(t *testing.T) {
	t.Parallel()

	watcher := NewWatcher(closedWatcherStream{ctx: context.Background()})
	manager := NewManager(slog.Default(), nil)
	watcher.Cleanup(manager, slog.Default())

	watcher.CreateWatch(
		&pb.WatchCreateRequest{Key: []byte("key")},
		1,
		0,
		func() int64 { return 0 },
		func(rev int64) (int64, bool, sql.NullString, error) {
			return rev, false, sql.NullString{}, nil
		},
		slog.Default(),
	)

	watcher.RLock()
	got := len(watcher.watches)
	watcher.RUnlock()
	if got != 0 {
		t.Fatalf("WatchCount() = %d, want 0 after CreateWatch on closed watcher", got)
	}
}

// TestDistributeDoesNotShareEventPointersAcrossResponses verifies that each
// queued WatchResponse owns its Event. Distribute sets PrevKv differently per
// watch, and WatchResponse copies only the Events slice header; reusing one
// *Event would let a later watch mutate an earlier queued response.
func TestDistributeDoesNotShareEventPointersAcrossResponses(t *testing.T) {
	t.Parallel()

	manager := NewManager(slog.Default(), nil)
	watcher := &Watcher{
		id:      1,
		inboxOk: true,
		inboxCh: make(chan pb.WatchResponse, 2),
		watches: map[int64]watchEntry{
			10: {key: []byte("key"), prevKv: true},
			20: {key: []byte("key"), prevKv: false},
		},
		progress: map[int64]bool{},
	}
	manager.Register(watcher)

	manager.Distribute(
		&proto.Record{Revision: 2, Key: []byte("key"), Value: []byte("new")},
		&proto.Record{Revision: 1, Key: []byte("key"), Value: []byte("old")},
	)

	responses := map[int64]pb.WatchResponse{}
	for i := 0; i < 2; i++ {
		select {
		case msg := <-watcher.inboxCh:
			responses[msg.WatchId] = msg
		default:
			t.Fatalf("received %d responses, want 2", i)
		}
	}

	for watchID, msg := range responses {
		gotPrevKV := len(msg.Events) == 1 && msg.Events[0].PrevKv != nil
		wantPrevKV := watchID == 10
		if gotPrevKV != wantPrevKV {
			t.Errorf("watch %d: gotPrevKV=%v, wantPrevKV=%v", watchID, gotPrevKV, wantPrevKV)
		}
	}
	if responses[10].Events[0] == responses[20].Events[0] {
		t.Fatal("queued watch responses share the same Event pointer")
	}
}
