// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"errors"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/nodestate"
	"github.com/netsy-dev/netsy/internal/proto"
)

// TestDegradeSelfTransitionsHealth verifies a Replica self-degrades when asked
// to move itself to the Degraded health state.
func TestDegradeSelfTransitionsHealth(t *testing.T) {
	follower, _, state := newTestFollower(t)

	follower.degradeSelf("receipt send failed after retry", errors.New("send failed"))

	if state.Health() != nodestate.HealthDegraded {
		t.Fatalf("Health() = %s, want %s", state.Health(), nodestate.HealthDegraded)
	}
}

// TestHandleCommitLagSelfDegrades verifies a Replica self-degrades when it
// remains behind the Primary's committed revision beyond the grace period.
func TestHandleCommitLagSelfDegrades(t *testing.T) {
	follower, _, state := newTestFollower(t)

	follower.handleCommit(3)
	time.Sleep(committedRevisionLagGracePeriod + 250*time.Millisecond)

	if state.Health() != nodestate.HealthDegraded {
		t.Fatalf("Health() = %s, want %s", state.Health(), nodestate.HealthDegraded)
	}
}

// TestHandleCommitLagClearsAfterCatchup verifies the lag timer is cleared when
// the Replica catches up before the grace period expires.
func TestHandleCommitLagClearsAfterCatchup(t *testing.T) {
	follower, db, state := newTestFollower(t)

	follower.handleCommit(1)

	time.Sleep(committedRevisionLagGracePeriod / 2)

	if _, err := db.ReplicateRecord(&proto.Record{
		Revision: 1,
		Key:      []byte("key"),
		Value:    []byte("value"),
		Created:  true,
		LeaderId: "primary-a",
	}); err != nil {
		t.Fatalf("ReplicateRecord() error = %v", err)
	}

	time.Sleep((committedRevisionLagGracePeriod / 2) + 250*time.Millisecond)

	if state.Health() != nodestate.HealthHealthy {
		t.Fatalf("Health() = %s, want %s", state.Health(), nodestate.HealthHealthy)
	}
}

// TestHandleCommitSkipsLagCheckWhileNotHealthy verifies live commit lag
// checking is disabled until the node reaches Healthy state.
func TestHandleCommitSkipsLagCheckWhileNotHealthy(t *testing.T) {
	follower, _, state := newTestFollower(t)

	if err := state.SetHealth(nodestate.HealthDegraded); err != nil {
		t.Fatalf("SetHealth(Degraded) error = %v", err)
	}
	if err := state.SetHealth(nodestate.HealthLoading); err != nil {
		t.Fatalf("SetHealth(Loading) error = %v", err)
	}

	follower.handleCommit(4)
	time.Sleep(committedRevisionLagGracePeriod + 250*time.Millisecond)

	if state.Health() != nodestate.HealthLoading {
		t.Fatalf("Health() = %s, want %s", state.Health(), nodestate.HealthLoading)
	}
	if state.Committed() != 4 {
		t.Fatalf("Committed() = %d, want 4", state.Committed())
	}
}
