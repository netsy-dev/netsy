// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/netsy-dev/netsy/internal/nodestate"
)

// Replica tracks a Replica's health and heartbeat state as seen by the
// Primary. Fields use atomic operations so they can be updated
// independently without holding a lock. HealthState and PrimaryState are
// stored as int32 because Go atomic operations require numeric types.
type Replica struct {
	NodeID         string
	LastHeartbeat  atomic.Int64 // unix nano — last standalone or Receipt-embedded heartbeat
	LastReceipt    atomic.Int64 // unix nano — last successful Receipt for a transaction
	ReceiptCount   atomic.Int64 // total Receipts; must reach 1 before counted as healthy for quorum
	HealthState    atomic.Int32 // int32 representation of nodestate.HealthState
	PrimaryState   atomic.Int32 // int32 representation of nodestate.PrimaryState
	LatestRevision atomic.Int64
}

// int32 representations for HealthState and PrimaryState, used with atomic
// operations on Replica fields.
const (
	healthLoading  int32 = 0
	healthHealthy  int32 = 1
	healthDegraded int32 = 2

	primaryReplica  int32 = 0
	primaryStarting int32 = 1
	primaryActive   int32 = 2
	primaryDraining int32 = 3
)

// Health returns the Replica's health state.
func (r *Replica) Health() nodestate.HealthState {
	switch r.HealthState.Load() {
	case healthHealthy:
		return nodestate.HealthHealthy
	case healthDegraded:
		return nodestate.HealthDegraded
	default:
		return nodestate.HealthLoading
	}
}

// SetHealth atomically updates the Replica's health state.
func (r *Replica) SetHealth(h nodestate.HealthState) {
	switch h {
	case nodestate.HealthHealthy:
		r.HealthState.Store(healthHealthy)
	case nodestate.HealthDegraded:
		r.HealthState.Store(healthDegraded)
	default:
		r.HealthState.Store(healthLoading)
	}
}

// Primary returns the Replica's primary state.
func (r *Replica) Primary() nodestate.PrimaryState {
	switch r.PrimaryState.Load() {
	case primaryStarting:
		return nodestate.PrimaryStarting
	case primaryActive:
		return nodestate.PrimaryActive
	case primaryDraining:
		return nodestate.PrimaryDraining
	default:
		return nodestate.PrimaryReplica
	}
}

// SetPrimary atomically updates the Replica's primary state.
func (r *Replica) SetPrimary(p nodestate.PrimaryState) {
	switch p {
	case nodestate.PrimaryStarting:
		r.PrimaryState.Store(primaryStarting)
	case nodestate.PrimaryActive:
		r.PrimaryState.Store(primaryActive)
	case nodestate.PrimaryDraining:
		r.PrimaryState.Store(primaryDraining)
	default:
		r.PrimaryState.Store(primaryReplica)
	}
}

// Replicas tracks connected Replicas for quorum and heartbeat
// monitoring. The Primary uses this to determine which Replicas are
// healthy for quorum transactions and to detect missed heartbeats.
type Replicas struct {
	mu       sync.RWMutex
	replicas map[string]*Replica
}

// NewReplicas creates an empty Replicas tracker.
func NewReplicas() *Replicas {
	return &Replicas{
		replicas: make(map[string]*Replica),
	}
}

// Add registers a Replica in the map. If the Replica already exists, the
// entry is replaced — resetting ReceiptCount to zero so the Replica must
// receipt at least once before being counted toward quorum.
func (m *Replicas) Add(nodeID string) *Replica {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry := &Replica{NodeID: nodeID}
	entry.LastHeartbeat.Store(time.Now().UnixNano())
	m.replicas[nodeID] = entry
	return entry
}

// Remove removes a Replica from the map.
func (m *Replicas) Remove(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.replicas, nodeID)
}

// Get returns the entry for a Replica. The second return value indicates
// whether the Replica was found.
func (m *Replicas) Get(nodeID string) (*Replica, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	e, ok := m.replicas[nodeID]
	return e, ok
}

// All returns all Replica entries.
func (m *Replicas) All() []*Replica {
	m.mu.RLock()
	defer m.mu.RUnlock()

	entries := make([]*Replica, 0, len(m.replicas))
	for _, e := range m.replicas {
		entries = append(entries, e)
	}
	return entries
}

// HealthyCount returns the number of Replicas currently reporting the Healthy
// health state, regardless of whether they have receipted a write yet.
func (m *Replicas) HealthyCount() int {
	count := 0
	for _, entry := range m.All() {
		if entry.Health() == nodestate.HealthHealthy {
			count++
		}
	}
	return count
}

// ReceiptedCount returns the number of Replicas that have successfully sent at
// least one Receipt to the Primary.
func (m *Replicas) ReceiptedCount() int {
	count := 0
	for _, entry := range m.All() {
		if entry.ReceiptCount.Load() > 0 {
			count++
		}
	}
	return count
}

// HealthyForQuorumCount returns the number of Replicas eligible to count
// toward quorum decisions: they must be Healthy and have receipted at least
// once.
func (m *Replicas) HealthyForQuorumCount() int {
	count := 0
	for _, entry := range m.All() {
		if entry.Health() != nodestate.HealthHealthy {
			continue
		}
		if entry.ReceiptCount.Load() == 0 {
			continue
		}
		count++
	}
	return count
}

// UpdateHeartbeat updates a Replica's heartbeat timestamp and state
// atomically. This is the single code path shared by standalone
// Heartbeats and Receipt-embedded Heartbeats. It returns false if the
// Replica is not in the map.
func (m *Replicas) UpdateHeartbeat(nodeID string, health nodestate.HealthState, primary nodestate.PrimaryState, latestRevision int64) bool {
	m.mu.RLock()
	e, ok := m.replicas[nodeID]
	m.mu.RUnlock()
	if !ok {
		return false
	}

	e.LastHeartbeat.Store(time.Now().UnixNano())
	e.SetHealth(health)
	e.SetPrimary(primary)
	e.LatestRevision.Store(latestRevision)
	return true
}

// UpdateReceipt updates a Replica's heartbeat timestamp, state, and
// receipt tracking atomically. Called when a Receipt is received from a
// Replica via the Follow replication stream. It returns false if the
// Replica is not in the map.
func (m *Replicas) UpdateReceipt(nodeID string, health nodestate.HealthState, primary nodestate.PrimaryState, latestRevision int64) bool {
	m.mu.RLock()
	e, ok := m.replicas[nodeID]
	m.mu.RUnlock()
	if !ok {
		return false
	}

	now := time.Now().UnixNano()
	e.LastHeartbeat.Store(now)
	e.SetHealth(health)
	e.SetPrimary(primary)
	e.LatestRevision.Store(latestRevision)
	e.LastReceipt.Store(now)
	e.ReceiptCount.Add(1)
	return true
}

// Reset clears all Replica entries. Called when this node loses Primary
// leadership.
func (m *Replicas) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.replicas = make(map[string]*Replica)
}
