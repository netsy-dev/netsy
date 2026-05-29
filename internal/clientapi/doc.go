// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

// Package clientapi implements the etcd-compatible gRPC Client API,
// including KV (Range, Txn), Watch, Lease, Cluster, and Maintenance
// services. Replicas proxy write requests to the Primary via the
// Peer API.
package clientapi
