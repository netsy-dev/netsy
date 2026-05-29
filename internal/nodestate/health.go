// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package nodestate

// HealthState represents the health of a Node.
type HealthState string

const (
	HealthLoading  HealthState = "loading"  // initial startup and backfill
	HealthHealthy  HealthState = "healthy"  // loading complete, not degraded
	HealthDegraded HealthState = "degraded" // failed heartbeat/receipt, missed heartbeats, or revision lag
)

// validHealthTransitions defines the set of allowed state changes.
var validHealthTransitions = map[HealthState][]HealthState{
	HealthLoading:  {HealthHealthy, HealthDegraded},
	HealthHealthy:  {HealthDegraded},
	HealthDegraded: {HealthLoading, HealthHealthy},
}

func validHealthTransition(from, to HealthState) bool {
	for _, allowed := range validHealthTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}
