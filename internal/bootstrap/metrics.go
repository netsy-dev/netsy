// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package bootstrap

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds always-on bootstrap/loading Prometheus metrics.
type Metrics struct {
	LoadingStageDuration *prometheus.HistogramVec
	LoadingRestarts      *prometheus.CounterVec
	LocalDBRebuilds      *prometheus.CounterVec
	RegistrationDuration *prometheus.HistogramVec
}

// NewMetrics creates all bootstrap-related Prometheus metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		LoadingStageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_loading_stage_duration_seconds",
			Help:    "Duration of individual loading stages.",
			Buckets: prometheus.DefBuckets,
		}, []string{"stage", "result"}),

		LoadingRestarts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_loading_restarts_total",
			Help: "Number of times the loading flow restarts.",
		}, []string{"reason"}),

		LocalDBRebuilds: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_local_db_rebuilds_total",
			Help: "Number of times a Node discards or rebuilds local database state.",
		}, []string{"reason"}),

		RegistrationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_node_registration_duration_seconds",
			Help:    "Duration of a Node's registration attempt with the Elector.",
			Buckets: prometheus.DefBuckets,
		}, []string{"result"}),
	}
}

// Collectors returns all collectors for registration with the registry.
func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.LoadingStageDuration,
		m.LoadingRestarts,
		m.LocalDBRebuilds,
		m.RegistrationDuration,
	}
}
