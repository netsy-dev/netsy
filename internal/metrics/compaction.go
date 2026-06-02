// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// CompactionMetrics holds the always-on local compaction execution
// histogram. Primary-scoped compaction coordination metrics live in the
// primary package.
type CompactionMetrics struct {
	Duration *prometheus.HistogramVec
}

// NewCompactionMetrics creates the always-on compaction duration metric.
func NewCompactionMetrics() *CompactionMetrics {
	return &CompactionMetrics{
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_compaction_duration_seconds",
			Help:    "Duration of local compaction work after a compaction revision is accepted.",
			Buckets: prometheus.DefBuckets,
		}, []string{"result"}),
	}
}

// Collectors returns all collectors for registration with the registry.
func (m *CompactionMetrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{m.Duration}
}
