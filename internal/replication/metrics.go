// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package replication

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds Replica-scoped replication Prometheus metrics.
type Metrics struct {
	ReceiptAge *receiptAgeGauge
}

// NewMetrics creates Replica-scoped replication metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		ReceiptAge: newReceiptAgeGauge(),
	}
}

// Collectors returns all Replica-scoped collectors for registration
// with a RoleGroup.
func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{m.ReceiptAge}
}

// receiptAgeGauge computes the receipt age at collect-time.
type receiptAgeGauge struct {
	desc     *prometheus.Desc
	mu       sync.Mutex
	lastSent time.Time
}

func newReceiptAgeGauge() *receiptAgeGauge {
	return &receiptAgeGauge{
		desc: prometheus.NewDesc(
			"netsy_replica_receipt_age_seconds",
			"Seconds since this Node last sent a Receipt to the Primary.",
			nil, nil,
		),
	}
}

// MarkSent records the current time as the last receipt send.
func (g *receiptAgeGauge) MarkSent() {
	g.mu.Lock()
	g.lastSent = time.Now()
	g.mu.Unlock()
}

// Describe sends the descriptor for this gauge.
func (g *receiptAgeGauge) Describe(ch chan<- *prometheus.Desc) {
	ch <- g.desc
}

// Collect computes and sends the current gauge value.
func (g *receiptAgeGauge) Collect(ch chan<- prometheus.Metric) {
	g.mu.Lock()
	last := g.lastSent
	g.mu.Unlock()
	var age float64
	if !last.IsZero() {
		age = time.Since(last).Seconds()
	}
	ch <- prometheus.MustNewConstMetric(g.desc, prometheus.GaugeValue, age)
}
