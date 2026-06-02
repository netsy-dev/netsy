// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package mtls

import (
	"crypto/x509"
	"net/url"
	"testing"
)

func TestParseURISAN(t *testing.T) {
	tests := []struct {
		name    string
		uris    []*url.URL
		wantID  *Identity
		wantErr bool
	}{
		{
			name:   "peer URI SAN",
			uris:   []*url.URL{BuildURISAN("my-cluster", RolePeer, "node-1")},
			wantID: &Identity{ClusterID: "my-cluster", Role: RolePeer, Name: "node-1"},
		},
		{
			name:   "client URI SAN",
			uris:   []*url.URL{BuildURISAN("my-cluster", RoleClient, "kubectl")},
			wantID: &Identity{ClusterID: "my-cluster", Role: RoleClient, Name: "kubectl"},
		},
		{
			name:    "no URI SANs",
			uris:    nil,
			wantErr: true,
		},
		{
			name:    "non-netsy URI only",
			uris:    []*url.URL{{Scheme: "spiffe", Host: "cluster", Path: "/peer/node-1"}},
			wantErr: true,
		},
		{
			name:    "empty cluster_id",
			uris:    []*url.URL{{Scheme: "netsy", Host: "", Path: "/peer/node-1"}},
			wantErr: true,
		},
		{
			name:    "missing identity segment",
			uris:    []*url.URL{{Scheme: "netsy", Host: "my-cluster", Path: "/peer"}},
			wantErr: true,
		},
		{
			name:    "too many path segments",
			uris:    []*url.URL{{Scheme: "netsy", Host: "my-cluster", Path: "/peer/node-1/extra"}},
			wantErr: true,
		},
		{
			name:    "invalid role",
			uris:    []*url.URL{{Scheme: "netsy", Host: "my-cluster", Path: "/admin/node-1"}},
			wantErr: true,
		},
		{
			name:    "empty identity",
			uris:    []*url.URL{{Scheme: "netsy", Host: "my-cluster", Path: "/peer/"}},
			wantErr: true,
		},
		{
			name:   "netsy URI after non-netsy URI",
			uris:   []*url.URL{{Scheme: "spiffe", Host: "x", Path: "/y"}, BuildURISAN("my-cluster", RolePeer, "node-1")},
			wantID: &Identity{ClusterID: "my-cluster", Role: RolePeer, Name: "node-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cert := &x509.Certificate{URIs: tt.uris}
			got, err := ParseURISAN(cert)
			if tt.wantErr {
				if err == nil {
					t.Fatal("ParseURISAN() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseURISAN() error = %v", err)
			}
			if *got != *tt.wantID {
				t.Fatalf("ParseURISAN() = %+v, want %+v", *got, *tt.wantID)
			}
		})
	}
}

func TestBuildURISAN(t *testing.T) {
	u := BuildURISAN("my-cluster", RolePeer, "node-1")
	want := "netsy://my-cluster/peer/node-1"
	if u.String() != want {
		t.Fatalf("BuildURISAN() = %q, want %q", u.String(), want)
	}
}
