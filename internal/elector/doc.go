// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

// Package elector integrates s3lect leader election into the Netsy node
// lifecycle. It manages the s3lect Elector, the dedicated HTTPS election
// health server, and wires leadership changes into the local node state.
package elector
