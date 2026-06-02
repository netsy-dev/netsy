// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds Primary-scoped Prometheus metrics. These are registered
// through a RoleGroup and disappear from scrape output when the node is
// not the Primary.
type Metrics struct {
	// Write path
	WritePath         *prometheus.GaugeVec
	WriteTransactions *prometheus.CounterVec
	WriteDuration     *prometheus.HistogramVec
	QuorumRollbacks   *prometheus.CounterVec
	RequiredReceipts  prometheus.Gauge
	HealthyReplicas   prometheus.Gauge
	ReceiptedReplicas prometheus.Gauge

	// Chunk buffer
	ChunkBufferRecords  prometheus.Gauge
	ChunkBufferBytes    prometheus.Gauge
	ChunkBufferAge      *collectTimeGauge
	ChunkBufferFlushes  *prometheus.CounterVec
	ChunkBufferFlushDur *prometheus.HistogramVec

	// Replication
	ReplicationStreams prometheus.Gauge

	// Drain
	DrainDuration *prometheus.HistogramVec

	// Preflight
	PreflightStageDur *prometheus.HistogramVec

	// Object storage revision
	ObjectStorageRevision prometheus.Gauge

	// Compaction (Primary-scoped)
	CompactionsTotal          *prometheus.CounterVec
	CompactionCoordDuration   *prometheus.HistogramVec
	CompactionConfirmFailures *prometheus.CounterVec
}

// NewMetrics creates all Primary-scoped Prometheus metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		WritePath: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "netsy_primary_write_path",
			Help: "Current write path. Exactly one label value is 1.",
		}, []string{"path"}),

		WriteTransactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_primary_write_transactions_total",
			Help: "Total write transactions attempted by the Primary.",
		}, []string{"path", "result"}),

		WriteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_primary_write_duration_seconds",
			Help:    "End-to-end write transaction duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"path", "result"}),

		QuorumRollbacks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_primary_quorum_rollbacks_total",
			Help: "Number of quorum transaction rollbacks.",
		}, []string{"reason"}),

		RequiredReceipts: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_primary_required_receipts",
			Help: "Current Replica Receipt threshold for quorum writes.",
		}),

		HealthyReplicas: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_primary_healthy_replicas",
			Help: "Number of Replicas currently counted as healthy for quorum.",
		}),

		ReceiptedReplicas: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_primary_receipted_replicas",
			Help: "Number of Replicas that have receipted at least once.",
		}),

		ChunkBufferRecords: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_primary_chunk_buffer_records",
			Help: "Number of records currently in the Chunk Buffer.",
		}),

		ChunkBufferBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_primary_chunk_buffer_bytes",
			Help: "Size in bytes of records in the Chunk Buffer.",
		}),

		ChunkBufferAge: newCollectTimeGauge(
			"netsy_primary_chunk_buffer_age_seconds",
			"Age in seconds of the oldest unflushed record. 0 when empty.",
		),

		ChunkBufferFlushes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_primary_chunk_buffer_flushes_total",
			Help: "Chunk buffer flush attempts.",
		}, []string{"trigger", "result"}),

		ChunkBufferFlushDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_primary_chunk_buffer_flush_duration_seconds",
			Help:    "Chunk buffer flush duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"trigger", "result"}),

		ReplicationStreams: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_primary_replication_streams",
			Help: "Number of currently connected replication streams.",
		}),

		DrainDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_primary_drain_duration_seconds",
			Help:    "Time spent draining before stepping down or exiting.",
			Buckets: prometheus.DefBuckets,
		}, []string{"result"}),

		PreflightStageDur: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_primary_preflight_stage_duration_seconds",
			Help:    "Duration of Primary preflight stages.",
			Buckets: prometheus.DefBuckets,
		}, []string{"stage", "result"}),

		ObjectStorageRevision: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "netsy_primary_object_storage_revision",
			Help: "Highest revision known to be durably written to object storage.",
		}),

		CompactionsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_primary_compactions_total",
			Help: "Compaction coordination runs initiated by the Primary.",
		}, []string{"result"}),

		CompactionCoordDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_primary_compaction_coordination_duration_seconds",
			Help:    "Duration of a cluster-wide compaction coordination run.",
			Buckets: prometheus.DefBuckets,
		}, []string{"result"}),

		CompactionConfirmFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_primary_compaction_confirmation_failures_total",
			Help: "Failed compaction confirmations by reason.",
		}, []string{"reason"}),
	}
}

// Collectors returns all Primary-scoped collectors for registration
// with a RoleGroup.
func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.WritePath,
		m.WriteTransactions,
		m.WriteDuration,
		m.QuorumRollbacks,
		m.RequiredReceipts,
		m.HealthyReplicas,
		m.ReceiptedReplicas,
		m.ChunkBufferRecords,
		m.ChunkBufferBytes,
		m.ChunkBufferAge,
		m.ChunkBufferFlushes,
		m.ChunkBufferFlushDur,
		m.ReplicationStreams,
		m.DrainDuration,
		m.PreflightStageDur,
		m.ObjectStorageRevision,
		m.CompactionsTotal,
		m.CompactionCoordDuration,
		m.CompactionConfirmFailures,
	}
}

// SetWritePath sets exactly one write path label to 1 and the other to 0.
func (m *Metrics) SetWritePath(path string) {
	for _, p := range []string{"sync", "quorum"} {
		if p == path {
			m.WritePath.WithLabelValues(p).Set(1)
		} else {
			m.WritePath.WithLabelValues(p).Set(0)
		}
	}
}
