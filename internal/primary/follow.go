// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"io"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/netsy-dev/netsy/internal/mtls"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/proto"
)

// streamBufferSize is the per-replica outbound message buffer. If a
// replica falls behind and fills this buffer, its stream is dropped
// rather than blocking leader writes.
const streamBufferSize = 256

// followSession represents a single active Follow stream to a replica.
type followSession struct {
	nodeID string
	sendCh chan *proto.PrimaryMessage
}

// addFollowStream registers a new follow session for the given node,
// closing any existing session for the same node ID.
func (s *Server) addFollowStream(nodeID string) *followSession {
	s.followMu.Lock()
	defer s.followMu.Unlock()

	if old, ok := s.followStreams[nodeID]; ok {
		close(old.sendCh)
	}

	session := &followSession{
		nodeID: nodeID,
		sendCh: make(chan *proto.PrimaryMessage, streamBufferSize),
	}
	s.followStreams[nodeID] = session
	return session
}

// removeFollowStream removes and closes the session for the given node
// ID, but only if it matches the provided session pointer (prevents
// removing a newer session that replaced this one).
func (s *Server) removeFollowStream(nodeID string, session *followSession) {
	s.followMu.Lock()
	defer s.followMu.Unlock()

	if current, ok := s.followStreams[nodeID]; ok && current == session {
		close(current.sendCh)
		delete(s.followStreams, nodeID)
	}
}

// broadcastToFollowers enqueues a message to all active follow sessions.
// If a session's buffer is full the message is dropped for that session
// (the degradation loop will eventually mark the replica as unhealthy).
func (s *Server) broadcastToFollowers(msg *proto.PrimaryMessage) {
	s.followMu.RLock()
	defer s.followMu.RUnlock()

	for _, session := range s.followStreams {
		select {
		case session.sendCh <- msg:
		default:
			// Buffer full — drop message. The degradation loop will
			// handle marking the replica as unhealthy.
		}
	}
}

// resetFollowStreams closes all follow sessions and clears the map.
// Called when this node loses Primary leadership.
func (s *Server) resetFollowStreams() {
	s.followMu.Lock()
	defer s.followMu.Unlock()

	for nodeID, session := range s.followStreams {
		close(session.sendCh)
		delete(s.followStreams, nodeID)
	}
}

// Follow implements the Primary.Follow bidirectional streaming RPC.
// Each Replica opens a Follow stream to receive PrimaryMessages and
// send ReplicaMessages (receipts with embedded heartbeats).
func (s *Server) Follow(stream proto.Primary_FollowServer) error {
	if err := s.requirePrimary(); err != nil {
		return err
	}

	nodeID, err := mtls.PeerNodeID(stream.Context())
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "failed to identify peer: %v", err)
	}

	logger := s.logger.With("replica", nodeID)
	logger.Info("follow stream opened")

	// Register the replica in the health tracker and the follow streams.
	s.replicas.Add(nodeID)
	session := s.addFollowStream(nodeID)
	if s.metrics != nil {
		s.metrics.ReplicationStreams.Inc()
	}

	defer func() {
		s.removeFollowStream(nodeID, session)
		s.replicas.Remove(nodeID)
		if s.metrics != nil {
			s.metrics.ReplicationStreams.Dec()
		}
		logger.Info("follow stream closed")
	}()

	// Send the initial message with current committed and compaction
	// revisions.
	initial := &proto.PrimaryMessage{
		Payload: &proto.PrimaryMessage_Initial{
			Initial: &proto.Initial{
				CommittedRevision:  s.state.Committed(),
				CompactionRevision: s.state.Compaction(),
			},
		},
	}
	if err := stream.Send(initial); err != nil {
		return err
	}

	// Start the send loop in a separate goroutine. It reads from the
	// session channel and forwards messages to the stream.
	sendErr := make(chan error, 1)
	go func() {
		sendErr <- s.followSendLoop(stream, session, logger)
	}()

	// Receive loop — process inbound receipts from the replica.
	recvErr := s.followRecvLoop(stream, nodeID, logger)

	// Wait for send loop to finish (it will end when the session
	// channel is closed or the stream context is cancelled).
	select {
	case err := <-sendErr:
		if err != nil && recvErr == nil {
			return err
		}
	default:
	}

	return recvErr
}

// followSendLoop reads messages from the session channel and sends them
// on the stream. It returns when the channel is closed or sending fails.
func (s *Server) followSendLoop(stream proto.Primary_FollowServer, session *followSession, logger *slog.Logger) error {
	for msg := range session.sendCh {
		if err := stream.Send(msg); err != nil {
			logger.Warn("follow send failed", "error", err)
			return err
		}
	}
	return nil
}

// followRecvLoop reads ReplicaMessages from the stream and processes
// receipts. It returns when the stream ends or an error occurs.
func (s *Server) followRecvLoop(stream proto.Primary_FollowServer, nodeID string, logger *slog.Logger) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		hb := msg.GetHeartbeat()
		if hb == nil {
			logger.Warn("receipt missing embedded heartbeat")
			continue
		}
		if hb.GetNodeId() != nodeID {
			return status.Errorf(codes.InvalidArgument,
				"heartbeat node_id %q does not match stream identity %q",
				hb.GetNodeId(), nodeID)
		}

		health := nodestate.HealthFromProto(hb.GetHealthState())
		primary := nodestate.PrimaryFromProto(hb.GetPrimaryState())

		if msg.GetRevision() > 0 {
			s.replicas.UpdateReceipt(nodeID, health, primary, hb.GetLatestRevision())
			s.collectReceipt(nodeID, msg.GetRevision())
		} else {
			s.replicas.UpdateHeartbeat(nodeID, health, primary, hb.GetLatestRevision())
		}
	}
}

// BroadcastRecord sends a record to all connected replicas via the
// follow streams.
func (s *Server) BroadcastRecord(record *proto.Record) {
	s.broadcastToFollowers(&proto.PrimaryMessage{
		Payload: &proto.PrimaryMessage_Record{
			Record: record,
		},
	})
}

// BroadcastCommit sends a committed_revision update to all connected
// replicas via the follow streams.
func (s *Server) BroadcastCommit(committedRevision int64) {
	s.broadcastToFollowers(&proto.PrimaryMessage{
		Payload: &proto.PrimaryMessage_Commit{
			Commit: committedRevision,
		},
	})
}

// BroadcastCompact sends a compaction_revision update to all connected
// replicas via the follow streams.
func (s *Server) BroadcastCompact(compactionRevision int64) {
	s.broadcastToFollowers(&proto.PrimaryMessage{
		Payload: &proto.PrimaryMessage_Compact{
			Compact: compactionRevision,
		},
	})
}
