// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package snapshot

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds Primary-scoped snapshot Prometheus metrics. These are
// registered through a RoleGroup and disappear from scrape output when
// the node is not the Primary.
type Metrics struct {
	Creations *prometheus.CounterVec
	CreateDur *prometheus.HistogramVec
	Age       *snapshotAgeGauge
}

// NewMetrics creates all snapshot-scoped Prometheus metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		Creations: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_primary_snapshot_creations_total",
			Help: "Snapshot creation attempts by the Primary.",
		}, []string{"result"}),

		CreateDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_primary_snapshot_creation_duration_seconds",
			Help:    "End-to-end snapshot creation duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"result"}),

		Age: newSnapshotAgeGauge(),
	}
}

// Collectors returns all snapshot-scoped collectors for registration
// with a RoleGroup.
func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.Creations,
		m.CreateDur,
		m.Age,
	}
}

// snapshotAgeGauge computes the snapshot age at collect-time.
type snapshotAgeGauge struct {
	desc        *prometheus.Desc
	mu          sync.Mutex
	lastCreated time.Time
}

func newSnapshotAgeGauge() *snapshotAgeGauge {
	return &snapshotAgeGauge{
		desc: prometheus.NewDesc(
			"netsy_primary_snapshot_age_seconds",
			"Seconds since the last successful snapshot was created.",
			nil, nil,
		),
	}
}

// MarkCreated records the current time as the last successful snapshot.
func (g *snapshotAgeGauge) MarkCreated() {
	g.mu.Lock()
	g.lastCreated = time.Now()
	g.mu.Unlock()
}

// Describe sends the descriptor for this gauge.
func (g *snapshotAgeGauge) Describe(ch chan<- *prometheus.Desc) {
	ch <- g.desc
}

// Collect computes and sends the current gauge value.
func (g *snapshotAgeGauge) Collect(ch chan<- prometheus.Metric) {
	g.mu.Lock()
	last := g.lastCreated
	g.mu.Unlock()
	var age float64
	if !last.IsZero() {
		age = time.Since(last).Seconds()
	}
	ch <- prometheus.MustNewConstMetric(g.desc, prometheus.GaugeValue, age)
}
