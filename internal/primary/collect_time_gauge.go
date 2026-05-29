// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package primary

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// collectTimeGauge is a prometheus.Collector that computes its value at
// collect-time by calling a user-supplied function. This avoids stale
// values and unnecessary background updates.
type collectTimeGauge struct {
	desc *prometheus.Desc
	mu   sync.Mutex
	fn   func() float64
}

func newCollectTimeGauge(name, help string) *collectTimeGauge {
	return &collectTimeGauge{
		desc: prometheus.NewDesc(name, help, nil, nil),
		fn:   func() float64 { return 0 },
	}
}

// SetFunc sets the function called at collect-time to produce the gauge value.
func (g *collectTimeGauge) SetFunc(fn func() float64) {
	g.mu.Lock()
	g.fn = fn
	g.mu.Unlock()
}

// Describe sends the descriptor for this gauge.
func (g *collectTimeGauge) Describe(ch chan<- *prometheus.Desc) {
	ch <- g.desc
}

// Collect computes and sends the current gauge value.
func (g *collectTimeGauge) Collect(ch chan<- prometheus.Metric) {
	g.mu.Lock()
	fn := g.fn
	g.mu.Unlock()
	ch <- prometheus.MustNewConstMetric(g.desc, prometheus.GaugeValue, fn())
}
