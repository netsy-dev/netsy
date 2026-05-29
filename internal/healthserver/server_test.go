// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package healthserver

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/netsy-dev/netsy/internal/nodestate"
)

func TestHealthEndpoint(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(*nodestate.State)
		wantStatus int
		wantBody   string
	}{
		{
			name:       "loading returns 503",
			setup:      func(s *nodestate.State) {},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "loading",
		},
		{
			name: "healthy returns 200",
			setup: func(s *nodestate.State) {
				_ = s.SetHealth(nodestate.HealthHealthy)
			},
			wantStatus: http.StatusOK,
			wantBody:   "healthy",
		},
		{
			name: "degraded returns 503",
			setup: func(s *nodestate.State) {
				_ = s.SetHealth(nodestate.HealthHealthy)
				_ = s.SetHealth(nodestate.HealthDegraded)
			},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "degraded",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := nodestate.New(slog.Default())
			tt.setup(state)

			s := &Server{
				logger: slog.Default(),
				state:  state,
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/health", nil)
			s.handleHealth(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("expected status %d, got %d", tt.wantStatus, rec.Code)
			}

			var resp healthResponse
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}
			if resp.Status != tt.wantBody {
				t.Fatalf("expected status %q, got %q", tt.wantBody, resp.Status)
			}
		})
	}
}
