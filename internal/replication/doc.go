// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

// Package replication implements the Replica-side Follow stream client.
// A Replica connects to the Primary, receives PrimaryMessages (records,
// commits, and compaction updates), persists records locally, and sends
// Receipts with embedded Heartbeats back to the Primary.
package replication
