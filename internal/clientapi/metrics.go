// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package clientapi

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds always-on Client API Prometheus metrics.
type Metrics struct {
	RequestsTotal    *prometheus.CounterVec
	RequestDuration  *prometheus.HistogramVec
	Watchers         prometheus.Gauge
	Watches          prometheus.Gauge
	WatchMinRevision prometheus.Gauge

	// ProxyRequestsTotal and ProxyRequestDuration are Replica-scoped.
	// They are registered through a separate RoleGroup.
	ProxyRequestsTotal   *prometheus.CounterVec
	ProxyRequestDuration *prometheus.HistogramVec
}

// NewMetrics creates all Client API Prometheus metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_client_requests_total",
			Help: "Client API requests handled by this Node.",
		}, []string{"kind", "result"}),

		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_client_request_duration_seconds",
			Help:    "Client API request duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"kind"}),

		Watchers: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_watchers",
			Help: "Number of connected Watchers on this Node.",
		}),

		Watches: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_watches",
			Help: "Number of active Watches on this Node.",
		}),

		WatchMinRevision: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_watch_min_revision",
			Help: "Minimum revision across active Watches on this Node.",
		}),

		ProxyRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_replica_proxy_requests_total",
			Help: "Write requests proxied by this Replica to the Primary.",
		}, []string{"kind", "result"}),

		ProxyRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_replica_proxy_request_duration_seconds",
			Help:    "Duration of proxied write requests from Replica perspective.",
			Buckets: prometheus.DefBuckets,
		}, []string{"kind"}),
	}
}

// AlwaysOnCollectors returns always-on collectors for direct registry
// registration.
func (m *Metrics) AlwaysOnCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.RequestsTotal,
		m.RequestDuration,
		m.Watchers,
		m.Watches,
		m.WatchMinRevision,
	}
}

// ReplicaCollectors returns Replica-scoped collectors for registration
// through a Replica RoleGroup.
func (m *Metrics) ReplicaCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.ProxyRequestsTotal,
		m.ProxyRequestDuration,
	}
}
