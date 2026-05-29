// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"log/slog"
	"sync"
	"time"

	"github.com/netsy-dev/netsy/internal/nodestate"
)

// NodeEntry represents a registered node in the Elector's node map.
type NodeEntry struct {
	NodeID                 string
	MemberID               uint64
	ClientAdvertiseAddress string
	PeerAdvertiseAddress   string
	LastHeartbeat          time.Time
	DegradedAt             time.Time
	HealthState            nodestate.HealthState
	PrimaryState           nodestate.PrimaryState
	LatestRevision         int64
	StartTime              int64
}

// NodeMap is the Elector's authoritative in-memory map of registered nodes.
type NodeMap struct {
	mu           sync.RWMutex
	logger       *slog.Logger
	nodes        map[string]*NodeEntry
	ready        bool
	deregistered map[string]struct{}
}

// NewNodeMap creates a new empty NodeMap.
func NewNodeMap(logger *slog.Logger) *NodeMap {
	return &NodeMap{
		logger:       logger,
		nodes:        make(map[string]*NodeEntry),
		deregistered: make(map[string]struct{}),
	}
}

// Add adds or updates a node entry in the map.
func (m *NodeMap) Add(entry NodeEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nodes[entry.NodeID] = &entry
	m.logger.Info("node added to map",
		"node_id", entry.NodeID,
		"member_id", entry.MemberID,
	)
}

// Remove removes a node entry from the map.
func (m *NodeMap) Remove(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.nodes, nodeID)
	m.logger.Info("node removed from map", "node_id", nodeID)
}

// Get returns a copy of a node entry. The second return value indicates
// whether the node was found.
func (m *NodeMap) Get(nodeID string) (entry NodeEntry, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	e, ok := m.nodes[nodeID]
	if !ok {
		return NodeEntry{}, false
	}
	return *e, true
}

// All returns copies of all node entries.
func (m *NodeMap) All() []NodeEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]NodeEntry, 0, len(m.nodes))
	for _, e := range m.nodes {
		entries = append(entries, *e)
	}
	return entries
}

// ForEach calls fn for each node entry while holding the read lock. The
// callback receives a copy of the entry. Avoid calling mutating NodeMap
// methods from within fn — use the returned node IDs to act on entries
// after iteration.
func (m *NodeMap) ForEach(fn func(NodeEntry)) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, e := range m.nodes {
		fn(*e)
	}
}

// Ready reports whether the bootstrap load has completed.
func (m *NodeMap) Ready() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.ready
}

// SetReady marks the node map as ready after bootstrap completes.
func (m *NodeMap) SetReady() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ready = true
}

// Reset clears all nodes and marks the map as not ready. This is called
// when the node loses Elector leadership.
func (m *NodeMap) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nodes = make(map[string]*NodeEntry)
	m.deregistered = make(map[string]struct{})
	m.ready = false
	m.logger.Info("node map reset")
}

// MarkDeregistered records that a node was deregistered during bootstrap,
// preventing the bootstrap loader from overwriting the deregistration with
// stale data from object storage.
func (m *NodeMap) MarkDeregistered(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deregistered[nodeID] = struct{}{}
}

// IsDeregistered reports whether the given node was deregistered during
// the current bootstrap cycle.
func (m *NodeMap) IsDeregistered(nodeID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.deregistered[nodeID]
	return ok
}

// ClearDeregistered clears the deregistered set after bootstrap completes.
func (m *NodeMap) ClearDeregistered() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deregistered = make(map[string]struct{})
}

// UpdateHeartbeat updates a node's heartbeat timestamp and state fields.
// It returns false if the node is not registered.
func (m *NodeMap) UpdateHeartbeat(nodeID string, t time.Time, health nodestate.HealthState, primary nodestate.PrimaryState, latestRevision int64, startTime int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.nodes[nodeID]
	if !ok {
		return false
	}
	e.LastHeartbeat = t
	e.HealthState = health
	e.PrimaryState = primary
	e.LatestRevision = latestRevision
	e.StartTime = startTime
	return true
}

// Count returns the number of registered nodes.
func (m *NodeMap) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.nodes)
}

// SetHealthState updates the health state for a node. When transitioning
// to Degraded, DegradedAt is set to the current time. It returns false
// if the node is not registered.
func (m *NodeMap) SetHealthState(nodeID string, health nodestate.HealthState) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.nodes[nodeID]
	if !ok {
		return false
	}
	e.HealthState = health
	if health == nodestate.HealthDegraded {
		e.DegradedAt = time.Now()
	} else {
		e.DegradedAt = time.Time{}
	}
	return true
}
