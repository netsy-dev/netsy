// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datastore

import (
	"strconv"
	"strings"
)

// FileInfo represents a chunk or snapshot file with its parsed revision
type FileInfo struct {
	Key      string
	Size     int64
	Revision int64
}

// parseRevisionFromKey extracts the revision number from a .netsy filename
func parseRevisionFromKey(key string) (int64, bool) {
	parts := strings.Split(key, "/")
	filename := parts[len(parts)-1]
	if !strings.HasSuffix(filename, ".netsy") {
		return 0, false
	}
	rev, err := strconv.ParseInt(strings.TrimSuffix(filename, ".netsy"), 10, 64)
	if err != nil {
		return 0, false
	}
	return rev, true
}
