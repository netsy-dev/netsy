// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package nodestate

// PrimaryState represents this Node's role relative to the Primary.
type PrimaryState string

const (
	PrimaryReplica  PrimaryState = "replica"  // not the Primary
	PrimaryStarting PrimaryState = "starting" // elected Primary, performing preflight checks
	PrimaryActive   PrimaryState = "active"   // accepting writes
	PrimaryDraining PrimaryState = "draining" // shutting down or failing, not accepting writes
)

// validPrimaryTransitions defines the set of allowed state changes.
// Replica -> Starting: elected by the Elector
// Starting -> Active: preflight checks complete
// Starting -> Replica: superseded by a new election before becoming active
// Starting -> Draining: shutdown signal received during preflight
// Active -> Draining: shutdown signal, chunk buffer full, or self-degradation
// Draining -> Replica: finished draining, giving up leadership
var validPrimaryTransitions = map[PrimaryState][]PrimaryState{
	PrimaryReplica:  {PrimaryStarting},
	PrimaryStarting: {PrimaryActive, PrimaryReplica, PrimaryDraining},
	PrimaryActive:   {PrimaryDraining},
	PrimaryDraining: {PrimaryReplica},
}

func validPrimaryTransition(from, to PrimaryState) bool {
	for _, allowed := range validPrimaryTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}
