// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package mtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/netsy-dev/netsy/internal/config"
)

// Role represents the mTLS certificate role embedded in URI SANs.
type Role string

const (
	RolePeer   Role = "peer"
	RoleClient Role = "client"
)

// ValidateLocalNodeCertificates checks the configured local peer certificates against node identity and advertised server addresses.
func ValidateLocalNodeCertificates(c *config.Config, tlsFiles *config.TLSFiles) error {
	if tlsFiles == nil {
		return fmt.Errorf("tls files missing")
	}
	if err := validatePeerIdentity(tlsFiles.ServerCert.Leaf, c.NodeID, c.ClusterID); err != nil {
		return fmt.Errorf("invalid server certificate identity: %w", err)
	}
	if err := validatePeerIdentity(tlsFiles.ClientCert.Leaf, c.NodeID, c.ClusterID); err != nil {
		return fmt.Errorf("invalid client certificate identity: %w", err)
	}
	if err := validateSANs(tlsFiles.ServerCert.Leaf, c.AdvertiseClient, c.AdvertisePeer, c.AdvertiseElection); err != nil {
		return fmt.Errorf("invalid server certificate SANs: %w", err)
	}
	return nil
}

// NewServerTLSConfig builds a server-side TLS configuration that only accepts client certificates with the given role.
func NewServerTLSConfig(c *config.Config, tlsFiles *config.TLSFiles, allowedRole Role) (*tls.Config, error) {
	if tlsFiles == nil {
		return nil, fmt.Errorf("tls files missing")
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		CipherSuites: []uint16{tls.TLS_AES_256_GCM_SHA384},
		Certificates: []tls.Certificate{
			*tlsFiles.ServerCert,
		},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  tlsFiles.ClientCA,
		VerifyConnection: func(state tls.ConnectionState) error {
			return verifyConnection(state, c.ClusterID, allowedRole)
		},
	}, nil
}

// verifyConnection checks the peer certificate's role and cluster identity on an established TLS connection.
func verifyConnection(state tls.ConnectionState, clusterID string, allowedRole Role) error {
	if len(state.VerifiedChains) == 0 {
		return fmt.Errorf("peer certificate was not verified")
	}
	if len(state.PeerCertificates) == 0 {
		return fmt.Errorf("peer certificate missing")
	}

	cert := state.PeerCertificates[0]
	id, err := ParseURISAN(cert)
	if err != nil {
		return err
	}
	if id.Role != allowedRole {
		return fmt.Errorf("certificate role %q is not allowed", id.Role)
	}
	if id.ClusterID != clusterID {
		return fmt.Errorf("certificate cluster_id must match %q, got %q", clusterID, id.ClusterID)
	}

	switch id.Role {
	case RolePeer:
		if err := config.ValidateIdentifier(id.Name, "peer certificate identity"); err != nil {
			return err
		}
	case RoleClient:
		if id.Name == "" {
			return fmt.Errorf("client certificate identity is required")
		}
	}

	return nil
}

// NewClientTLSConfig builds a client-side TLS configuration for outbound
// peer gRPC connections. The local node presents its client certificate
// and verifies the remote server against the cluster CA.
func NewClientTLSConfig(tlsFiles *config.TLSFiles) (*tls.Config, error) {
	if tlsFiles == nil {
		return nil, fmt.Errorf("tls files missing")
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		CipherSuites: []uint16{tls.TLS_AES_256_GCM_SHA384},
		Certificates: []tls.Certificate{
			*tlsFiles.ClientCert,
		},
		RootCAs: tlsFiles.ServerCA,
	}, nil
}

// validatePeerIdentity checks that a local node certificate has the peer role and matches the expected node and cluster identity.
func validatePeerIdentity(cert *x509.Certificate, nodeID, clusterID string) error {
	if cert == nil {
		return fmt.Errorf("certificate missing")
	}
	id, err := ParseURISAN(cert)
	if err != nil {
		return err
	}
	if id.ClusterID != clusterID {
		return fmt.Errorf("certificate cluster_id must match %q, got %q", clusterID, id.ClusterID)
	}
	if id.Role != RolePeer {
		return fmt.Errorf("certificate role must be %q, got %q", RolePeer, id.Role)
	}
	if id.Name != nodeID {
		return fmt.Errorf("certificate identity must match node_id %q, got %q", nodeID, id.Name)
	}
	return nil
}

// validateSANs checks that the server certificate's SANs cover every configured advertise address.
func validateSANs(cert *x509.Certificate, advertiseAddrs ...string) error {
	if cert == nil {
		return fmt.Errorf("certificate missing")
	}

	seenHosts := make(map[string]struct{})
	for _, addr := range advertiseAddrs {
		if addr == "" {
			continue
		}
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("invalid advertise address %q: %w", addr, err)
		}
		if host == "" {
			return fmt.Errorf("advertise address %q must include a host", addr)
		}
		if _, ok := seenHosts[host]; ok {
			continue
		}
		seenHosts[host] = struct{}{}
		if err := cert.VerifyHostname(host); err != nil {
			return fmt.Errorf("server certificate SANs do not cover advertise address %q: %w", addr, err)
		}
	}

	return nil
}

// PeerNodeID extracts the peer's node ID from the gRPC context's TLS peer certificate URI SAN.
func PeerNodeID(ctx context.Context) (string, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", fmt.Errorf("no peer info in context")
	}
	if p.AuthInfo == nil {
		return "", fmt.Errorf("no auth info in peer")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", fmt.Errorf("peer auth info is not TLS")
	}
	if len(tlsInfo.State.PeerCertificates) == 0 {
		return "", fmt.Errorf("no peer certificates")
	}
	id, err := ParseURISAN(tlsInfo.State.PeerCertificates[0])
	if err != nil {
		return "", err
	}
	return id.Name, nil
}
