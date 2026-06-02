// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// RoleGroup implements prometheus.Collector and wraps role-specific
// metrics. Metrics are only emitted in Collect when active() returns
// true, so they disappear from scrape output when the node leaves the
// corresponding role.
type RoleGroup struct {
	active     func() bool
	mu         sync.RWMutex
	collectors []prometheus.Collector
}

// NewRoleGroup creates a RoleGroup whose metrics are only collected
// when active returns true.
func NewRoleGroup(active func() bool) *RoleGroup {
	return &RoleGroup{active: active}
}

// Add appends collectors to this role group.
func (g *RoleGroup) Add(cs ...prometheus.Collector) {
	g.mu.Lock()
	g.collectors = append(g.collectors, cs...)
	g.mu.Unlock()
}

// Describe sends all descriptor information for every collector in the
// group. Prometheus requires descriptors to be emitted unconditionally.
func (g *RoleGroup) Describe(ch chan<- *prometheus.Desc) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, c := range g.collectors {
		c.Describe(ch)
	}
}

// Collect emits metric values only when the role is active. When the
// role is inactive, no metrics appear in scrape output.
func (g *RoleGroup) Collect(ch chan<- prometheus.Metric) {
	if !g.active() {
		return
	}
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, c := range g.collectors {
		c.Collect(ch)
	}
}
