// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/tidwall/jsonc"
)

// ClusterConfig holds per-cluster settings parsed from a JSONC config file.
type ClusterConfig struct {
	ClusterID          string            `json:"cluster_id"`
	Storage            StorageConfig     `json:"storage"`
	HeartbeatInterval  Duration          `json:"heartbeat_interval"`
	Elector            ElectorConfig     `json:"elector"`
	Replication        ReplicationConfig `json:"replication"`
	Snapshot           SnapshotConfig    `json:"snapshot"`
	CompactionInterval Duration          `json:"compaction_interval"`
}

type StorageConfig struct {
	Provider   string `json:"provider"` // "s3" or "gcs", default "s3"
	BucketName string `json:"bucket_name"`
	KeyPrefix  string `json:"key_prefix"`
	Class      string `json:"class"`      // default "STANDARD"
	Encryption string `json:"encryption"` // default "provider-managed"
	KMSKeyID   string `json:"kms_key_id"`
}

type ElectorConfig struct {
	DegradationCount      int      `json:"degradation_count"`      // default 2
	DeregistrationTimeout Duration `json:"deregistration_timeout"` // default "3m"
	PrimaryPriorTimeout   Duration `json:"primary_prior_timeout"`
}

type ReplicationConfig struct {
	Quorum           *int              `json:"quorum"`            // default -1; 0 = disabled, -1 = majority, positive = static
	DegradationCount int               `json:"degradation_count"` // default 2
	ChunkBuffer      ChunkBufferConfig `json:"chunk_buffer"`
}

type ChunkBufferConfig struct {
	ThresholdSizeMB     int `json:"threshold_size_mb"`
	ThresholdAgeMinutes int `json:"threshold_age_minutes"`
}

type SnapshotConfig struct {
	ThresholdRecords    int64 `json:"threshold_records"`     // default 10000
	ThresholdSizeMB     int64 `json:"threshold_size_mb"`     // default 10000
	ThresholdAgeMinutes int64 `json:"threshold_age_minutes"` // default 0
}

// LoadClusterConfig reads a JSONC config file, strips comments, and unmarshals it.
func LoadClusterConfig(path string) (ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ClusterConfig{}, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	// Strip JSONC comments
	data = jsonc.ToJSON(data)

	var cc ClusterConfig
	if err := json.Unmarshal(data, &cc); err != nil {
		return ClusterConfig{}, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	// Apply defaults
	if cc.Storage.Provider == "" {
		cc.Storage.Provider = "s3"
	}
	if cc.Storage.Class == "" {
		cc.Storage.Class = "STANDARD"
	}
	if cc.Storage.Encryption == "" {
		cc.Storage.Encryption = "provider-managed"
	}
	if cc.Elector.DegradationCount == 0 {
		cc.Elector.DegradationCount = 2
	}
	if cc.Elector.DeregistrationTimeout.Duration == 0 {
		cc.Elector.DeregistrationTimeout.Duration = 3 * time.Minute
	}
	if cc.Replication.Quorum == nil {
		defaultQuorum := -1
		cc.Replication.Quorum = &defaultQuorum
	}
	if cc.Replication.DegradationCount == 0 {
		cc.Replication.DegradationCount = 2
	}
	if cc.Snapshot.ThresholdRecords == 0 {
		cc.Snapshot.ThresholdRecords = 10000
	}
	if cc.Snapshot.ThresholdSizeMB == 0 {
		cc.Snapshot.ThresholdSizeMB = 10000
	}

	return cc, nil
}
