// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

// Package primary implements the Primary gRPC service and domain layer,
// including transaction processing, the Follow replication stream, chunk
// buffering for async object storage writes, Replica heartbeat and receipt
// tracking, and heartbeat-based degradation.
package primary
