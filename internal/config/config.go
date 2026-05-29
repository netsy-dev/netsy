// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"hash/fnv"
)

// Config combines per-node settings (from env vars) and per-cluster settings
// (from a JSONC config file).
type Config struct {
	NodeConfig
	ClusterConfig

	// EtcdClusterID is a stable FNV-1a 64-bit hash of the string ClusterID,
	// computed once at load time for use in etcd ResponseHeader.ClusterId.
	EtcdClusterID uint64
}

// FlagOverrides holds CLI flag values that override env-var defaults.
type FlagOverrides struct {
	ConfigPath string // --config flag
	Verbose    bool   // --verbose flag
}

// Load builds a fully validated Config from environment variables, the JSONC
// cluster config file, and any CLI flag overrides. It returns an error if the
// config file cannot be found/parsed or validation fails.
func Load(flags FlagOverrides) (*Config, error) {
	node := LoadNodeConfig()

	if flags.ConfigPath != "" {
		node.ConfigPath = flags.ConfigPath
	}
	if flags.Verbose {
		node.Verbose = true
	}

	if node.ConfigPath == "" {
		return nil, fmt.Errorf("config file path required (use --config flag or NETSY_CONFIG env var)")
	}

	cluster, err := LoadClusterConfig(node.ConfigPath)
	if err != nil {
		return nil, err
	}

	c := &Config{NodeConfig: node, ClusterConfig: cluster}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	c.EtcdClusterID = hashClusterID(c.ClusterID)
	return c, nil
}

// hashClusterID returns a stable FNV-1a 64-bit hash of the cluster ID string
// for use as the etcd-compatible numeric cluster identifier in ResponseHeader.
func hashClusterID(clusterID string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(clusterID))
	return h.Sum64()
}

// ValidateFile loads and validates a config file without starting the server.
// Node config is still loaded from env vars (and flag overrides) so that
// cross-cutting validations such as node_id can run.
func ValidateFile(path string, flags FlagOverrides) error {
	node := LoadNodeConfig()
	if flags.Verbose {
		node.Verbose = true
	}

	cluster, err := LoadClusterConfig(path)
	if err != nil {
		return err
	}

	c := &Config{NodeConfig: node, ClusterConfig: cluster}
	return c.Validate()
}
