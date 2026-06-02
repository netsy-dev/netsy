// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package nodestate

import (
	"github.com/prometheus/client_golang/prometheus"
)

// StateMetrics holds always-on node state and revision gauges. It
// implements StateObserver so it is automatically updated on state
// transitions.
type StateMetrics struct {
	Info               *prometheus.GaugeVec
	HealthState        *prometheus.GaugeVec
	ElectorState       *prometheus.GaugeVec
	PrimaryState       *prometheus.GaugeVec
	ProcessStartTime   prometheus.Gauge
	LatestRevision     prometheus.Gauge
	CommittedRevision  prometheus.Gauge
	CompactionRevision prometheus.Gauge
}

// NewStateMetrics creates the always-on node state metrics and
// initialises the info gauge to 1.
func NewStateMetrics(version, clusterID, nodeID, quorumConfig string) *StateMetrics {
	m := &StateMetrics{
		Info: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "netsy_info",
			Help: "Build and configuration info. Always 1.",
		}, []string{"version", "cluster_id", "node_id", "quorum_config"}),

		HealthState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "netsy_state_health",
			Help: "Current Health State. Exactly one label value is 1.",
		}, []string{"state"}),

		ElectorState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "netsy_state_elector",
			Help: "Current Elector State. Exactly one label value is 1.",
		}, []string{"state"}),

		PrimaryState: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "netsy_state_primary",
			Help: "Current Primary State. Exactly one label value is 1.",
		}, []string{"state"}),

		ProcessStartTime: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_process_start_time_seconds",
			Help: "Unix timestamp when this Netsy process started.",
		}),

		LatestRevision: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_latest_revision",
			Help: "Highest revision present in this Node's local SQLite database.",
		}),

		CommittedRevision: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_committed_revision",
			Help: "Current committed_revision on this Node.",
		}),

		CompactionRevision: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_compaction_revision",
			Help: "Latest accepted compaction revision on this Node.",
		}),
	}

	m.Info.With(prometheus.Labels{
		"version":       version,
		"cluster_id":    clusterID,
		"node_id":       nodeID,
		"quorum_config": quorumConfig,
	}).Set(1)

	// Initialise state gauges to Loading / Follower / Replica.
	m.SetHealth("loading")
	m.SetElector("follower")
	m.SetPrimary("replica")

	return m
}

// Collectors returns all collectors for registration with the registry.
func (m *StateMetrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.Info,
		m.HealthState,
		m.ElectorState,
		m.PrimaryState,
		m.ProcessStartTime,
		m.LatestRevision,
		m.CommittedRevision,
		m.CompactionRevision,
	}
}

// SetHealth sets exactly one health state label to 1 and all others to 0.
func (m *StateMetrics) SetHealth(state string) {
	for _, s := range []string{"loading", "healthy", "degraded"} {
		if s == state {
			m.HealthState.WithLabelValues(s).Set(1)
		} else {
			m.HealthState.WithLabelValues(s).Set(0)
		}
	}
}

// SetElector sets exactly one elector state label to 1 and all others to 0.
func (m *StateMetrics) SetElector(state string) {
	for _, s := range []string{"follower", "leader"} {
		if s == state {
			m.ElectorState.WithLabelValues(s).Set(1)
		} else {
			m.ElectorState.WithLabelValues(s).Set(0)
		}
	}
}

// SetPrimary sets exactly one primary state label to 1 and all others to 0.
func (m *StateMetrics) SetPrimary(state string) {
	for _, s := range []string{"replica", "starting", "active", "draining"} {
		if s == state {
			m.PrimaryState.WithLabelValues(s).Set(1)
		} else {
			m.PrimaryState.WithLabelValues(s).Set(0)
		}
	}
}
