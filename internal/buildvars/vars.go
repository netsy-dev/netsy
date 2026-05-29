// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package buildvars

// EtcdCompatVersion is the etcd API version that Netsy aims to be
// compatible with. This is returned in the StatusResponse.Version
// field so etcd clients can identify the compatibility target.
const EtcdCompatVersion = "3.5.21"

// set during build time
var (
	buildVersion = ""
	buildDate    = ""
	commitHash   = ""
	commitDate   = ""
	commitBranch = ""
)

// BuildVersion returns immutable build version
func BuildVersion() string {
	return buildVersion
}

// BuildDate returns immutable build date
func BuildDate() string {
	return buildDate
}

// CommitHash returns immutable git commit hash
func CommitHash() string {
	return commitHash
}

// CommitDate returns immutable build date
func CommitDate() string {
	return commitDate
}

// CommitBranch returns immutable commit branch
func CommitBranch() string {
	return commitBranch
}
