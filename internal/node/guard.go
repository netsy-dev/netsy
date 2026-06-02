// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package node

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// splitBrainWindow is the minimum time that must pass after a handoff
// before the previous Elector is allowed to push again. If the
// previous Elector returns within this window it indicates two
// concurrent Electors rather than a normal re-election.
const splitBrainWindow = 30 * time.Second

// ElectorGuard detects potential split-brain scenarios where two
// Electors are concurrently pushing cluster state to the same Node.
//
// Detection works by tracking the current and previous Elector IDs
// along with the timestamp of the most recent handoff. If the
// previous Elector pushes again within splitBrainWindow of being
// superseded, the guard rejects the push and logs a split-brain
// error. After the window elapses, the same sequence (A → B → A) is
// treated as a normal re-election.
type ElectorGuard struct {
	logger *slog.Logger

	mu              sync.Mutex
	currentElector  string
	previousElector string
	handoffAt       time.Time // when currentElector superseded previousElector
}

// NewElectorGuard creates a new split-brain guard.
func NewElectorGuard(logger *slog.Logger) *ElectorGuard {
	return &ElectorGuard{logger: logger}
}

// Check validates that the given elector node ID is consistent with
// the expected single-Elector model. If the previous Elector pushes
// again within splitBrainWindow of being superseded, it indicates two
// concurrent Electors and the push is rejected. After the window
// elapses, the returning Elector is accepted as a normal re-election.
func (g *ElectorGuard) Check(electorNodeID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// First push or same elector — nothing to detect.
	if g.currentElector == "" || g.currentElector == electorNodeID {
		g.currentElector = electorNodeID
		return nil
	}

	// A different elector is pushing. Check if the previous elector
	// has come back within the split-brain window.
	if electorNodeID == g.previousElector && time.Since(g.handoffAt) < splitBrainWindow {
		g.logger.Error("potential elector split-brain detected: previous elector is still active",
			"current", g.currentElector,
			"rejected", electorNodeID,
			"handoff_age", time.Since(g.handoffAt).Round(time.Millisecond),
		)
		return fmt.Errorf("split-brain: elector %s was superseded by %s %s ago but is still pushing",
			electorNodeID, g.currentElector, time.Since(g.handoffAt).Round(time.Millisecond))
	}

	g.logger.Info("elector changed",
		"previous", g.currentElector,
		"new", electorNodeID,
	)

	g.previousElector = g.currentElector
	g.currentElector = electorNodeID
	g.handoffAt = time.Now()
	return nil
}
