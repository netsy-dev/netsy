// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// RetryMetrics tracks retry attempts across all operations.
type RetryMetrics struct {
	Total *prometheus.CounterVec
}

// NewRetryMetrics creates the always-on retry counter.
func NewRetryMetrics() *RetryMetrics {
	return &RetryMetrics{
		Total: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_retries_total",
			Help: "Retry attempts by operation.",
		}, []string{"operation"}),
	}
}

// Collectors returns all collectors for registration with the registry.
func (m *RetryMetrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{m.Total}
}

// Inc increments the retry counter for the given operation.
func (m *RetryMetrics) Inc(operation string) {
	m.Total.WithLabelValues(operation).Inc()
}
