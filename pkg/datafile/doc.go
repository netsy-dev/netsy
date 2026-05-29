// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

// Package datafile provides a public API for external tools that need to
// read and write Netsy .netsy data files.
//
// This package intentionally exposes plain Go types instead of generated
// protobuf types. Netsy internals should continue to use internal/datafile
//
// Record byte slices are not defensively copied. Callers must not mutate them
// concurrently with read or write operations.
package datafile
