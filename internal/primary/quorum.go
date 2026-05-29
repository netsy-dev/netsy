// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"sync"
	"time"

	"github.com/netsy-dev/netsy/internal/nodestate"
)

// txnStrategy captures the per-transaction decision of which commit path to
// use and how many Replica Receipts are needed. When useQuorum is true the
// Primary follows Path 2 (quorum Receipts); otherwise Path 1 (sync object
// storage).
type txnStrategy struct {
	useQuorum        bool // true = async object storage writes, false = sync
	requiredReceipts int
}

// requiredReceipts calculates the number of Replica Receipts needed for a
// quorum transaction based on the quorum configuration and the total number
// of registered Nodes (including the Primary).
//
// For majority quorum (-1), the Primary's own durable SQLite commit counts
// as one participant, so the required Replica Receipts are floor(N/2).
// For static quorum (positive int), the value is used directly.
// For disabled quorum (0), zero is returned.
func requiredReceipts(quorum int, nodeCount int) int {
	switch {
	case quorum == 0:
		return 0
	case quorum > 0:
		return quorum
	default:
		// Majority: floor(N/2) where N includes the Primary.
		if nodeCount <= 1 {
			return 0
		}
		return nodeCount / 2
	}
}

// selectTxnStrategy determines which transaction strategy to use for the
// current write based on the quorum configuration, registered node count,
// and the number of Replicas currently eligible for quorum.
func selectTxnStrategy(quorum int, nodeCount int, healthyForQuorum int) txnStrategy {
	required := requiredReceipts(quorum, nodeCount)
	if required <= 0 || healthyForQuorum < required {
		return txnStrategy{}
	}
	return txnStrategy{useQuorum: true, requiredReceipts: required}
}

// receiptCollector tracks Receipts for a single in-flight quorum
// transaction. Because leaderTxnGate serializes all writes, at most one
// receiptCollector is active at any time.
type receiptCollector struct {
	revision int64
	required int

	mu     sync.Mutex
	acked  map[string]struct{}
	doneCh chan struct{}
	done   bool
}

// newReceiptCollector creates a collector for the given revision that
// completes once the required number of distinct Replica Receipts arrive.
func newReceiptCollector(revision int64, required int) *receiptCollector {
	return &receiptCollector{
		revision: revision,
		required: required,
		acked:    make(map[string]struct{}),
		doneCh:   make(chan struct{}),
	}
}

// collectReceipt records that a Replica has receipted the quorum
// transaction. When the required threshold is met, the collector completes.
func (c *receiptCollector) collectReceipt(nodeID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done {
		return
	}

	c.acked[nodeID] = struct{}{}
	if len(c.acked) >= c.required {
		c.done = true
		close(c.doneCh)
	}
}

// wait blocks until either the quorum threshold is met or the timeout
// elapses. It returns true when quorum was achieved.
func (c *receiptCollector) wait(timeout time.Duration) bool {
	select {
	case <-c.doneCh:
		return true
	case <-time.After(timeout):
		return false
	}
}

// isComplete reports whether the required Receipt threshold has been met.
func (c *receiptCollector) isComplete() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// unackedQuorumNodeIDs returns the node IDs of Replicas that were eligible
// for quorum (healthy and previously receipted) but did not send a Receipt
// for this transaction. Only these Replicas should be marked as degraded
// after a quorum timeout.
func (c *receiptCollector) unackedQuorumNodeIDs(all []*Replica) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var missing []string
	for _, r := range all {
		if _, ok := c.acked[r.NodeID]; ok {
			continue
		}
		if r.Health() != nodestate.HealthHealthy || r.ReceiptCount.Load() == 0 {
			continue
		}
		missing = append(missing, r.NodeID)
	}
	return missing
}
