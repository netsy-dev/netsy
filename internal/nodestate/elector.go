// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package nodestate

// ElectorState represents whether this Node is the cluster Elector.
type ElectorState string

const (
	ElectorFollower ElectorState = "follower" // not the Elector
	ElectorLeader   ElectorState = "leader"   // currently the Elector
)

// validElectorTransitions defines the set of allowed state changes.
var validElectorTransitions = map[ElectorState][]ElectorState{
	ElectorFollower: {ElectorLeader},
	ElectorLeader:   {ElectorFollower},
}

func validElectorTransition(from, to ElectorState) bool {
	for _, allowed := range validElectorTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}
