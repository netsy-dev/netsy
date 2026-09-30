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

	CleanupRuns           *prometheus.CounterVec
	CleanupDur            *prometheus.HistogramVec
	ChunksListed          prometheus.Counter
	ChunksRequested       prometheus.Counter
	ChunksDeleted         prometheus.Counter
	ChunksFailed          prometheus.Counter
	CleanupQueuedRevision prometheus.Gauge
	CleanupLastRevision   prometheus.Gauge
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

		CleanupRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_primary_snapshot_cleanup_runs_total",
			Help: "Chunk cleanup runs by the Primary, by result.",
		}, []string{"result"}),

		CleanupDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_primary_snapshot_cleanup_duration_seconds",
			Help:    "End-to-end chunk cleanup run duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"result"}),

		ChunksListed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "netsy_primary_snapshot_cleanup_chunks_listed_total",
			Help: "Chunk objects listed for cleanup.",
		}),
		ChunksRequested: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "netsy_primary_snapshot_cleanup_chunks_requested_total",
			Help: "Delete requests sent in direct mode, without confirming the key existed.",
		}),
		ChunksDeleted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "netsy_primary_snapshot_cleanup_chunks_deleted_total",
			Help: "Chunk objects successfully deleted by cleanup.",
		}),
		ChunksFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "netsy_primary_snapshot_cleanup_chunks_failed_total",
			Help: "Chunk object deletions that failed during cleanup.",
		}),

		CleanupQueuedRevision: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_primary_snapshot_cleanup_queued_revision",
			Help: "Highest revision currently enqueued for chunk cleanup.",
		}),
		CleanupLastRevision: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_primary_snapshot_cleanup_last_revision",
			Help: "Highest revision fully cleaned so far.",
		}),
	}
}

// Collectors returns all snapshot-scoped collectors for registration
// with a RoleGroup.
func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.Creations,
		m.CreateDur,
		m.Age,
		m.CleanupRuns,
		m.CleanupDur,
		m.ChunksListed,
		m.ChunksRequested,
		m.ChunksDeleted,
		m.ChunksFailed,
		m.CleanupQueuedRevision,
		m.CleanupLastRevision,
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
