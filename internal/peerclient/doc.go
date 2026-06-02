// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

// Package peerclient manages outbound gRPC client connections to peer
// Nodes. It owns the current Elector and Primary connections and
// updates them when the cluster state changes.
package peerclient
