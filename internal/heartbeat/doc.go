// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

// Package heartbeat manages the outbound heartbeat sender goroutine.
// Each Node sends heartbeats to the Elector on a regular cadence and
// to the Primary when no Receipt has been sent within the replication
// heartbeat window.
package heartbeat
