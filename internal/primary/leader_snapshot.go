// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"time"
)

// checkAndCreateSnapshot checks if a snapshot should be created based on
// configured thresholds and creates one asynchronously if needed. The
// snapshot is gated on committed_revision so that only durably committed
// records are included. This should only be called by the Primary.
func (ps *Server) checkAndCreateSnapshot(currentRevision int64, recordSize int64) {
	if ps.snapshotWorker == nil {
		return
	}

	// Use committed revision as the snapshot ceiling to ensure only
	// durably committed records are captured.
	committedRevision := ps.state.Committed()
	if committedRevision <= 0 {
		return
	}

	ps.snapshotWorker.RequestSnapshot(committedRevision, time.Now(), recordSize)
}
