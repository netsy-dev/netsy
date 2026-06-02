// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

var identifierRegexp = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]*[a-z0-9])?$`)

// ValidateIdentifier checks that value is lowercase alphanumeric and hyphens,
// with no leading/trailing/consecutive hyphens, max 32 chars.
func ValidateIdentifier(value, fieldName string) error {
	if value == "" {
		return fmt.Errorf("%s is required", fieldName)
	}
	if len(value) > 32 {
		return fmt.Errorf("%s must be at most 32 characters", fieldName)
	}
	if strings.Contains(value, "--") {
		return fmt.Errorf("%s must not contain consecutive hyphens", fieldName)
	}
	if !identifierRegexp.MatchString(value) {
		return fmt.Errorf("%s must be lowercase alphanumeric with hyphens, no leading/trailing hyphens", fieldName)
	}
	return nil
}

// Validate validates all config fields and returns the first error found.
func (c *Config) Validate() error {
	// node_id
	if err := ValidateIdentifier(c.NodeID, "node_id"); err != nil {
		return err
	}

	// cluster_id
	if err := ValidateIdentifier(c.ClusterID, "cluster_id"); err != nil {
		return err
	}

	// storage.bucket_name
	if c.Storage.BucketName == "" {
		return fmt.Errorf("storage.bucket_name is required")
	}

	// storage.provider
	switch c.Storage.Provider {
	case "s3", "gcs":
		// valid
	default:
		return fmt.Errorf("storage.provider must be \"s3\" or \"gcs\", got %q", c.Storage.Provider)
	}

	// storage.encryption
	switch c.Storage.Encryption {
	case "provider-managed", "customer-managed":
		// valid
	default:
		return fmt.Errorf("storage.encryption must be \"provider-managed\" or \"customer-managed\", got %q", c.Storage.Encryption)
	}

	// storage.kms_key_id required when encryption is customer-managed
	if c.Storage.Encryption == "customer-managed" && c.Storage.KMSKeyID == "" {
		return fmt.Errorf("storage.kms_key_id is required when storage.encryption is \"customer-managed\"")
	}

	// Provider-specific storage.class validation — only classes suitable for
	// active read/write workloads are permitted; archive and cold classes have
	// retrieval delays or minimum storage durations that break backfill/recovery.
	switch c.Storage.Provider {
	case "s3":
		switch c.Storage.Class {
		case "STANDARD", "STANDARD_IA", "INTELLIGENT_TIERING":
			// valid S3 storage classes
		default:
			return fmt.Errorf("storage.class %q is not a valid S3 storage class (must be STANDARD, STANDARD_IA, or INTELLIGENT_TIERING)", c.Storage.Class)
		}
	case "gcs":
		switch c.Storage.Class {
		case "STANDARD":
			// valid GCS storage class
		default:
			return fmt.Errorf("storage.class %q is not a valid GCS storage class (must be STANDARD)", c.Storage.Class)
		}
	}

	// elector.degradation_count >= 1
	if c.Elector.DegradationCount < 1 {
		return fmt.Errorf("elector.degradation_count must be >= 1")
	}

	// replication.degradation_count >= 1
	if c.Replication.DegradationCount < 1 {
		return fmt.Errorf("replication.degradation_count must be >= 1")
	}

	// heartbeat_interval > 0
	if c.HeartbeatInterval.Duration <= 0 {
		return fmt.Errorf("heartbeat_interval must be > 0")
	}

	// elector.primary_prior_timeout >= degradation_count * heartbeat_interval
	minPriorTimeout := time.Duration(c.Elector.DegradationCount) * c.HeartbeatInterval.Duration
	if c.Elector.PrimaryPriorTimeout.Duration < minPriorTimeout {
		return fmt.Errorf("elector.primary_prior_timeout (%s) must be >= elector.degradation_count (%d) * heartbeat_interval (%s) = %s",
			c.Elector.PrimaryPriorTimeout.Duration,
			c.Elector.DegradationCount,
			c.HeartbeatInterval.Duration,
			minPriorTimeout,
		)
	}

	// replication.chunk_buffer.threshold_age_minutes > 0 when quorum != 0
	if *c.Replication.Quorum != 0 && c.Replication.ChunkBuffer.ThresholdAgeMinutes <= 0 {
		return fmt.Errorf("replication.chunk_buffer.threshold_age_minutes must be > 0 when replication.quorum != 0")
	}

	return nil
}
