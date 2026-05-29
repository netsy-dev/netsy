// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"context"
	"time"

	"github.com/netsy-dev/netsy/internal/nodestate"
)

// committedRevisionLagGracePeriod is the delay before a Replica self-degrades
// after learning the Primary has committed beyond the Replica's latest local
// revision.
const committedRevisionLagGracePeriod = 2 * time.Second

// scheduleCommittedRevisionLagCheck arms or clears the lag timer that
// self-degrades a Replica when it remains behind the Primary's committed
// revision after the grace period.
func (f *Follower) scheduleCommittedRevisionLagCheck(committedRevision int64) {
	if committedRevision <= 0 {
		f.cancelCommittedRevisionLagCheck()
		return
	}

	latestRevision, err := f.db.LatestRevision()
	if err != nil {
		f.logger.Warn("failed to evaluate committed revision lag",
			"committed_revision", committedRevision,
			"error", err,
		)
		return
	}

	if latestRevision >= committedRevision {
		f.cancelCommittedRevisionLagCheck()
		return
	}

	f.lagCheckMu.Lock()
	if f.lagCheckCancel != nil {
		f.lagCheckCancel()
	}

	f.lagCheckSeq++
	seq := f.lagCheckSeq
	lagCtx, cancel := context.WithCancel(context.Background())
	f.lagCheckCancel = cancel
	f.lagCheckMu.Unlock()

	go func(targetRevision int64, checkSeq uint64) {
		timer := time.NewTimer(committedRevisionLagGracePeriod)
		defer timer.Stop()

		select {
		case <-lagCtx.Done():
			return
		case <-timer.C:
		}

		latestRevision, err := f.db.LatestRevision()
		if err != nil {
			f.logger.Warn("failed to re-check committed revision lag",
				"committed_revision", targetRevision,
				"error", err,
			)
			f.clearCommittedRevisionLagCheck(checkSeq)
			return
		}

		if latestRevision < targetRevision {
			f.degradeSelf("committed revision lag exceeded grace period", nil)
			f.logger.Warn("replica remained behind committed revision",
				"committed_revision", targetRevision,
				"latest_revision", latestRevision,
				"grace_period", committedRevisionLagGracePeriod,
			)
		}

		f.clearCommittedRevisionLagCheck(checkSeq)
	}(committedRevision, seq)
}

// cancelCommittedRevisionLagCheck stops any outstanding committed-revision lag
// timer.
func (f *Follower) cancelCommittedRevisionLagCheck() {
	f.lagCheckMu.Lock()
	defer f.lagCheckMu.Unlock()

	if f.lagCheckCancel != nil {
		f.lagCheckCancel()
		f.lagCheckCancel = nil
	}
}

// clearCommittedRevisionLagCheck clears the active lag timer only when the
// caller still owns the latest scheduled check.
func (f *Follower) clearCommittedRevisionLagCheck(seq uint64) {
	f.lagCheckMu.Lock()
	defer f.lagCheckMu.Unlock()

	if seq == f.lagCheckSeq {
		f.lagCheckCancel = nil
	}
}

// degradeSelf transitions this Replica to Degraded once and logs the cause.
func (f *Follower) degradeSelf(reason string, cause error) {
	if f.state.Health() == nodestate.HealthDegraded {
		return
	}

	if err := f.state.SetHealth(nodestate.HealthDegraded); err != nil {
		f.logger.Warn("failed to self-degrade replica",
			"reason", reason,
			"cause", cause,
			"error", err,
		)
		return
	}

	f.logger.Warn("replica self-degraded",
		"reason", reason,
		"cause", cause,
	)
}
