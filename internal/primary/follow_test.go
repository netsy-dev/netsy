// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"log/slog"
	"testing"

	"github.com/netsy-dev/netsy/internal/proto"
)

func newTestServerForFollow() *Server {
	return &Server{
		logger:        slog.Default(),
		followStreams: make(map[string]*followSession),
	}
}

func TestFollowStreamAddAndBroadcast(t *testing.T) {
	s := newTestServerForFollow()

	s1 := s.addFollowStream("node-a")
	s2 := s.addFollowStream("node-b")

	msg := &proto.PrimaryMessage{
		Payload: &proto.PrimaryMessage_Commit{Commit: 42},
	}
	s.broadcastToFollowers(msg)

	select {
	case got := <-s1.sendCh:
		if got.GetCommit() != 42 {
			t.Fatalf("expected commit 42, got %d", got.GetCommit())
		}
	default:
		t.Fatal("expected message on node-a channel")
	}

	select {
	case got := <-s2.sendCh:
		if got.GetCommit() != 42 {
			t.Fatalf("expected commit 42, got %d", got.GetCommit())
		}
	default:
		t.Fatal("expected message on node-b channel")
	}
}

func TestFollowStreamReplaceSession(t *testing.T) {
	s := newTestServerForFollow()

	old := s.addFollowStream("node-a")
	newSession := s.addFollowStream("node-a")

	// Old channel should be closed.
	if _, ok := <-old.sendCh; ok {
		t.Fatal("expected old session channel to be closed")
	}

	// New session should still work.
	msg := &proto.PrimaryMessage{
		Payload: &proto.PrimaryMessage_Commit{Commit: 1},
	}
	s.broadcastToFollowers(msg)

	select {
	case got := <-newSession.sendCh:
		if got.GetCommit() != 1 {
			t.Fatalf("expected commit 1, got %d", got.GetCommit())
		}
	default:
		t.Fatal("expected message on new session channel")
	}
}

func TestFollowStreamRemove(t *testing.T) {
	s := newTestServerForFollow()

	session := s.addFollowStream("node-a")
	s.removeFollowStream("node-a", session)

	// Channel should be closed after remove.
	if _, ok := <-session.sendCh; ok {
		t.Fatal("expected channel to be closed after remove")
	}

	// Broadcast after remove should not panic.
	s.broadcastToFollowers(&proto.PrimaryMessage{
		Payload: &proto.PrimaryMessage_Commit{Commit: 1},
	})
}

func TestFollowStreamRemoveWrongSession(t *testing.T) {
	s := newTestServerForFollow()

	old := s.addFollowStream("node-a")
	current := s.addFollowStream("node-a")

	// Remove with old session pointer should not affect current session.
	s.removeFollowStream("node-a", old)

	msg := &proto.PrimaryMessage{
		Payload: &proto.PrimaryMessage_Commit{Commit: 5},
	}
	s.broadcastToFollowers(msg)

	select {
	case got := <-current.sendCh:
		if got.GetCommit() != 5 {
			t.Fatalf("expected commit 5, got %d", got.GetCommit())
		}
	default:
		t.Fatal("expected message on current session channel")
	}
}

func TestFollowStreamReset(t *testing.T) {
	s := newTestServerForFollow()

	s1 := s.addFollowStream("node-a")
	s2 := s.addFollowStream("node-b")

	s.resetFollowStreams()

	// Both channels should be closed.
	if _, ok := <-s1.sendCh; ok {
		t.Fatal("expected node-a channel to be closed")
	}
	if _, ok := <-s2.sendCh; ok {
		t.Fatal("expected node-b channel to be closed")
	}
}

func TestFollowStreamBroadcastDropsOnFullBuffer(t *testing.T) {
	s := newTestServerForFollow()
	session := s.addFollowStream("node-a")

	// Fill the buffer.
	for i := 0; i < streamBufferSize; i++ {
		s.broadcastToFollowers(&proto.PrimaryMessage{
			Payload: &proto.PrimaryMessage_Commit{Commit: int64(i)},
		})
	}

	// Next broadcast should not block — the message is dropped.
	s.broadcastToFollowers(&proto.PrimaryMessage{
		Payload: &proto.PrimaryMessage_Commit{Commit: 999},
	})

	// Drain and verify original messages are present.
	for i := 0; i < streamBufferSize; i++ {
		select {
		case got := <-session.sendCh:
			if got.GetCommit() != int64(i) {
				t.Fatalf("expected commit %d, got %d", i, got.GetCommit())
			}
		default:
			t.Fatalf("expected message %d in buffer", i)
		}
	}

	// The dropped message (999) should not be in the channel.
	select {
	case <-session.sendCh:
		t.Fatal("expected no more messages after buffer was full")
	default:
	}
}
