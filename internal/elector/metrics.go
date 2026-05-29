// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds Elector-scoped Prometheus metrics. These are registered
// through a RoleGroup and disappear from scrape output when the node is
// not the Elector.
type Metrics struct {
	RegisteredNodes         prometheus.Gauge
	HealthyNodes            prometheus.Gauge
	DegradedNodes           prometheus.Gauge
	PrimaryElections        *prometheus.CounterVec
	PrimaryElectionFailures *prometheus.CounterVec
	PrimaryElectionDuration *prometheus.HistogramVec
	PrimaryElectionContacts *prometheus.CounterVec
	AutoDeregistrations     prometheus.Counter
}

// NewMetrics creates all Elector-scoped Prometheus metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		RegisteredNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_elector_registered_nodes",
			Help: "Number of currently registered Nodes in the Elector's in-memory map.",
		}),
		HealthyNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_elector_healthy_nodes",
			Help: "Number of Nodes currently in Healthy Health State.",
		}),
		DegradedNodes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_elector_degraded_nodes",
			Help: "Number of Nodes currently in Degraded Health State.",
		}),
		PrimaryElections: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_elector_primary_elections_total",
			Help: "Primary elections run by this Node as the Elector.",
		}, []string{"result"}),
		PrimaryElectionFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_elector_primary_election_failures_total",
			Help: "Failed Primary elections by failure reason.",
		}, []string{"reason"}),
		PrimaryElectionDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_elector_primary_election_duration_seconds",
			Help:    "End-to-end Primary election duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"result"}),
		PrimaryElectionContacts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_elector_primary_election_contacts_total",
			Help: "Node contact attempts during Primary elections.",
		}, []string{"result"}),
		AutoDeregistrations: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "netsy_elector_auto_deregistrations_total",
			Help: "Nodes automatically deregistered after exceeding deregistration timeout.",
		}),
	}
}

// Collectors returns all Elector-scoped collectors for registration
// with a RoleGroup.
func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.RegisteredNodes,
		m.HealthyNodes,
		m.DegradedNodes,
		m.PrimaryElections,
		m.PrimaryElectionFailures,
		m.PrimaryElectionDuration,
		m.PrimaryElectionContacts,
		m.AutoDeregistrations,
	}
}
