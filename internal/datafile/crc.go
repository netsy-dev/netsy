// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package datafile

import (
	"hash/crc64"
)

var crcTable = crc64.MakeTable(crc64.ECMA)
