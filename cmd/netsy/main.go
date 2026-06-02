// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"

	"github.com/netsy-dev/netsy/internal/cmd"
)

func main() {
	err := cmd.NewRootCmd().Execute()
	if err != nil {
		os.Exit(1)
	}
}
