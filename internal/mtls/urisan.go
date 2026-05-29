// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package mtls

import (
	"crypto/x509"
	"fmt"
	"net/url"
	"strings"
)

// uriScheme is the URI scheme used for Netsy identity URIs.
const uriScheme = "netsy"

// Identity holds the parsed identity from a certificate's URI SAN.
type Identity struct {
	ClusterID string
	Role      Role
	Name      string // node_id for peers, arbitrary identifier for clients
}

// ParseURISAN extracts the Identity from the first netsy:// URI SAN on the certificate.
func ParseURISAN(cert *x509.Certificate) (*Identity, error) {
	for _, u := range cert.URIs {
		if u.Scheme != uriScheme {
			continue
		}
		clusterID := u.Host
		if clusterID == "" {
			return nil, fmt.Errorf("URI SAN %q has empty cluster_id", u.String())
		}
		parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
		if len(parts) != 2 {
			return nil, fmt.Errorf("URI SAN %q must have path /{role}/{identity}", u.String())
		}
		role := Role(parts[0])
		switch role {
		case RolePeer, RoleClient:
		default:
			return nil, fmt.Errorf("URI SAN %q has invalid role %q", u.String(), parts[0])
		}
		name := parts[1]
		if name == "" {
			return nil, fmt.Errorf("URI SAN %q has empty identity", u.String())
		}
		return &Identity{ClusterID: clusterID, Role: role, Name: name}, nil
	}
	return nil, fmt.Errorf("certificate has no netsy:// URI SAN")
}

// BuildURISAN constructs a netsy:// URI SAN for the given cluster, role, and identity.
func BuildURISAN(clusterID string, role Role, identity string) *url.URL {
	return &url.URL{
		Scheme: uriScheme,
		Host:   clusterID,
		Path:   "/" + string(role) + "/" + identity,
	}
}
