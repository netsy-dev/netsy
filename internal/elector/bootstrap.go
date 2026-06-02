// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/netsy-dev/netsy/internal/datastore"
	"github.com/netsy-dev/netsy/internal/discovery"
	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/storage"
)

// Bootstrap loads the node map from object storage when this node acquires
// Elector leadership. It reads members.json and node registration files,
// populating the in-memory node map. If members.json does not exist (first
// Elector), it promotes any bootstrap snapshot before creating members.json.
func (s *Server) Bootstrap(ctx context.Context) error {
	s.logger.Info("starting elector bootstrap")

	mf, readErr := discovery.ReadMembersFile(ctx, s.store)
	if readErr != nil && !errors.Is(readErr, storage.ErrNotFound) {
		return fmt.Errorf("read members file: %w", readErr)
	}
	firstElector := errors.Is(readErr, storage.ErrNotFound)

	regs, err := discovery.ListNodeRegistrations(ctx, s.store)
	if err != nil {
		return fmt.Errorf("list node registrations: %w", err)
	}

	if firstElector {
		if err := s.bootstrapFirstElector(ctx, regs); err != nil {
			return fmt.Errorf("first elector bootstrap: %w", err)
		}
	} else {
		if err := s.bootstrapExisting(ctx, mf, regs); err != nil {
			return fmt.Errorf("existing elector bootstrap: %w", err)
		}
	}

	s.nodeMap.ClearDeregistered()
	s.nodeMap.SetReady()

	entries := s.nodeMap.All()
	s.logger.Info("elector bootstrap complete", "node_count", len(entries))
	for _, e := range entries {
		s.logger.Info("bootstrapped node",
			"node_id", e.NodeID,
			"member_id", e.MemberID,
			"peer_addr", e.PeerAdvertiseAddress,
		)
	}

	return nil
}

// bootstrapFirstElector handles the case where no members.json exists yet. It
// first promotes any operator-provided bootstrap snapshot into normal snapshot
// history, then creates a new MembersFile, allocates member_ids for discovered
// node registration files, and writes the initial members.json.
func (s *Server) bootstrapFirstElector(ctx context.Context, regs []discovery.NodeRegistration) error {
	s.logger.Info("first elector bootstrap — checking for bootstrap snapshot",
		"discovered_nodes", len(regs),
	)

	bootstrapSnapshotInfo, err := datastore.PromoteBootstrapSnapshot(ctx, s.store, s.bootstrapTempDir)
	if err != nil {
		return fmt.Errorf("promote bootstrap snapshot: %w", err)
	}
	if bootstrapSnapshotInfo.Found {
		s.logger.Info("bootstrap snapshot promoted",
			"source_key", datastore.BootstrapSnapshotKey,
			"snapshot_key", bootstrapSnapshotInfo.Key,
			"revision", bootstrapSnapshotInfo.Revision,
		)
	}

	s.logger.Info("first elector bootstrap — creating members.json",
		"discovered_nodes", len(regs),
	)

	mf := discovery.MembersFile{
		ClusterID: s.clusterID,
		Members:   make(map[string]uint64),
	}

	for _, reg := range regs {
		if s.nodeMap.IsDeregistered(reg.NodeID) {
			s.logger.Info("skipping deregistered node during bootstrap",
				"node_id", reg.NodeID,
			)
			continue
		}

		id := discovery.AllocateMemberID(mf)
		mf.Members[reg.NodeID] = id

		s.nodeMap.Add(NodeEntry{
			NodeID:                 reg.NodeID,
			MemberID:               id,
			ClientAdvertiseAddress: reg.ClientAdvertiseAddress,
			PeerAdvertiseAddress:   reg.PeerAdvertiseAddress,
			LastHeartbeat:          time.Now(),
			HealthState:            nodestate.HealthLoading,
			PrimaryState:           nodestate.PrimaryReplica,
		})
	}

	if err := discovery.WriteMembersFile(ctx, s.store, mf); err != nil {
		return fmt.Errorf("write initial members file: %w", err)
	}

	return nil
}

// bootstrapExisting handles the case where members.json already exists.
// It reconciles node registration files with the members mapping, allocating
// new member_ids for any nodes not yet in members.json.
func (s *Server) bootstrapExisting(ctx context.Context, mf discovery.MembersFile, regs []discovery.NodeRegistration) error {
	s.logger.Info("existing elector bootstrap — loading from members.json",
		"member_count", len(mf.Members),
		"discovered_nodes", len(regs),
	)

	needsWrite := false

	for _, reg := range regs {
		if s.nodeMap.IsDeregistered(reg.NodeID) {
			s.logger.Info("skipping deregistered node during bootstrap",
				"node_id", reg.NodeID,
			)
			continue
		}

		if _, exists := s.nodeMap.Get(reg.NodeID); exists {
			continue
		}

		memberID, ok := discovery.FindMemberID(mf, reg.NodeID)
		if !ok {
			memberID = discovery.AllocateMemberID(mf)
			mf.Members[reg.NodeID] = memberID
			needsWrite = true
		}

		s.nodeMap.Add(NodeEntry{
			NodeID:                 reg.NodeID,
			MemberID:               memberID,
			ClientAdvertiseAddress: reg.ClientAdvertiseAddress,
			PeerAdvertiseAddress:   reg.PeerAdvertiseAddress,
			LastHeartbeat:          time.Now(),
			HealthState:            nodestate.HealthLoading,
			PrimaryState:           nodestate.PrimaryReplica,
		})
	}

	if needsWrite {
		if err := discovery.WriteMembersFile(ctx, s.store, mf); err != nil {
			return fmt.Errorf("write updated members file: %w", err)
		}
	}

	return nil
}
