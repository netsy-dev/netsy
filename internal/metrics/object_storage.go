// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// ObjectStorageMetrics holds Prometheus collectors for object storage
// operations.
type ObjectStorageMetrics struct {
	WritesTotal   *prometheus.CounterVec
	WriteDuration *prometheus.HistogramVec
	WriteBytes    *prometheus.HistogramVec
	ReadsTotal    *prometheus.CounterVec
	ReadDuration  *prometheus.HistogramVec
}

// NewObjectStorageMetrics creates object storage metric collectors.
func NewObjectStorageMetrics() *ObjectStorageMetrics {
	return &ObjectStorageMetrics{
		WritesTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_object_storage_writes_total",
			Help: "Object storage write attempts.",
		}, []string{"kind", "mode", "result"}),

		WriteDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_object_storage_write_duration_seconds",
			Help:    "Object storage write duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"kind", "mode", "result"}),

		WriteBytes: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_object_storage_write_bytes",
			Help:    "Payload size written to object storage.",
			Buckets: []float64{1024, 10240, 102400, 1048576, 10485760, 104857600},
		}, []string{"kind", "mode"}),

		ReadsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "netsy_object_storage_reads_total",
			Help: "Object storage read attempts.",
		}, []string{"result"}),

		ReadDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "netsy_object_storage_read_duration_seconds",
			Help:    "Object storage read duration.",
			Buckets: prometheus.DefBuckets,
		}, []string{"result"}),
	}
}

// Collectors returns all collectors for registration.
func (m *ObjectStorageMetrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.WritesTotal,
		m.WriteDuration,
		m.WriteBytes,
		m.ReadsTotal,
		m.ReadDuration,
	}
}

// ObserveWrite records write metrics for an object storage operation.
func (m *ObjectStorageMetrics) ObserveWrite(kind, mode string, bytes int64, duration time.Duration, err error) {
	result := resultLabel(err)
	m.WritesTotal.WithLabelValues(kind, mode, result).Inc()
	m.WriteDuration.WithLabelValues(kind, mode, result).Observe(duration.Seconds())
	m.WriteBytes.WithLabelValues(kind, mode).Observe(float64(bytes))
}

// ObserveRead records read metrics for an object storage operation.
func (m *ObjectStorageMetrics) ObserveRead(duration time.Duration, err error) {
	result := resultLabel(err)
	m.ReadsTotal.WithLabelValues(result).Inc()
	m.ReadDuration.WithLabelValues(result).Observe(duration.Seconds())
}

// resultLabel returns "success" or "error" for a metric label.
func resultLabel(err error) string {
	if err != nil {
		return "error"
	}
	return "success"
}
