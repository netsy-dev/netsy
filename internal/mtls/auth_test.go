// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package mtls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/netsy-dev/netsy/internal/config"
)

func TestValidateLocalNodeCertificates(t *testing.T) {
	serverCert := newLeaf(t, certSpec{
		commonName:   "node-1",
		uriSANs:      []*url.URL{BuildURISAN("my-cluster", RolePeer, "node-1")},
		dnsNames:     []string{"node-1.example.internal"},
		ipAddrs:      []net.IP{net.ParseIP("172.16.0.1")},
		extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	clientCert := newLeaf(t, certSpec{
		commonName:   "node-1",
		uriSANs:      []*url.URL{BuildURISAN("my-cluster", RolePeer, "node-1")},
		extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})

	err := ValidateLocalNodeCertificates(&config.Config{
		NodeConfig: config.NodeConfig{
			NodeID:            "node-1",
			AdvertiseClient:   "172.16.0.1:2378",
			AdvertisePeer:     "node-1.example.internal:2381",
			AdvertiseElection: "172.16.0.1:8443",
		},
		ClusterConfig: config.ClusterConfig{
			ClusterID: "my-cluster",
		},
	}, &config.TLSFiles{
		ServerCert: &tls.Certificate{Leaf: serverCert},
		ClientCert: &tls.Certificate{Leaf: clientCert},
	})
	if err != nil {
		t.Fatalf("ValidateLocalNodeCertificates() error = %v", err)
	}
}

func TestValidateLocalNodeCertificatesRejectsWrongNodeID(t *testing.T) {
	cert := newLeaf(t, certSpec{
		commonName:   "node-wrong",
		uriSANs:      []*url.URL{BuildURISAN("my-cluster", RolePeer, "node-wrong")},
		dnsNames:     []string{"localhost"},
		extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})

	err := ValidateLocalNodeCertificates(&config.Config{
		NodeConfig:    config.NodeConfig{NodeID: "node-1"},
		ClusterConfig: config.ClusterConfig{ClusterID: "my-cluster"},
	}, &config.TLSFiles{
		ServerCert: &tls.Certificate{Leaf: cert},
		ClientCert: &tls.Certificate{Leaf: cert},
	})
	if err == nil {
		t.Fatal("expected error for wrong node ID")
	}
}

func TestValidateLocalNodeCertificatesRejectsClientRole(t *testing.T) {
	cert := newLeaf(t, certSpec{
		commonName:   "node-1",
		uriSANs:      []*url.URL{BuildURISAN("my-cluster", RoleClient, "node-1")},
		dnsNames:     []string{"localhost"},
		extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})

	err := ValidateLocalNodeCertificates(&config.Config{
		NodeConfig:    config.NodeConfig{NodeID: "node-1"},
		ClusterConfig: config.ClusterConfig{ClusterID: "my-cluster"},
	}, &config.TLSFiles{
		ServerCert: &tls.Certificate{Leaf: cert},
		ClientCert: &tls.Certificate{Leaf: cert},
	})
	if err == nil {
		t.Fatal("expected error for client role on node certificate")
	}
}

func TestValidateLocalNodeCertificatesRejectsMissingSAN(t *testing.T) {
	serverCert := newLeaf(t, certSpec{
		commonName:   "node-1",
		uriSANs:      []*url.URL{BuildURISAN("my-cluster", RolePeer, "node-1")},
		dnsNames:     []string{"localhost"},
		extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	clientCert := newLeaf(t, certSpec{
		commonName:   "node-1",
		uriSANs:      []*url.URL{BuildURISAN("my-cluster", RolePeer, "node-1")},
		extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})

	err := ValidateLocalNodeCertificates(&config.Config{
		NodeConfig: config.NodeConfig{
			NodeID:          "node-1",
			AdvertiseClient: "172.16.0.1:2378",
		},
		ClusterConfig: config.ClusterConfig{ClusterID: "my-cluster"},
	}, &config.TLSFiles{
		ServerCert: &tls.Certificate{Leaf: serverCert},
		ClientCert: &tls.Certificate{Leaf: clientCert},
	})
	if err == nil {
		t.Fatal("expected error for missing SAN")
	}
}

func TestNewServerTLSConfig(t *testing.T) {
	serverCert := newLeaf(t, certSpec{
		commonName:   "node-1",
		uriSANs:      []*url.URL{BuildURISAN("my-cluster", RolePeer, "node-1")},
		extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	tlsFiles := &config.TLSFiles{
		ServerCert: &tls.Certificate{Leaf: serverCert},
		ClientCA:   x509.NewCertPool(),
	}
	conf := &config.Config{
		ClusterConfig: config.ClusterConfig{ClusterID: "my-cluster"},
	}

	t.Run("client role accepts client certs", func(t *testing.T) {
		cfg, err := NewServerTLSConfig(conf, tlsFiles, RoleClient)
		if err != nil {
			t.Fatalf("NewServerTLSConfig() error = %v", err)
		}
		err = cfg.VerifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				newLeaf(t, certSpec{
					commonName:   "kubectl",
					uriSANs:      []*url.URL{BuildURISAN("my-cluster", RoleClient, "kubectl")},
					extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				}),
			},
			VerifiedChains: [][]*x509.Certificate{{serverCert}},
		})
		if err != nil {
			t.Fatalf("VerifyConnection() error = %v", err)
		}
	})

	t.Run("rejects missing URI SAN", func(t *testing.T) {
		cfg, _ := NewServerTLSConfig(conf, tlsFiles, RoleClient)
		err := cfg.VerifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				newLeaf(t, certSpec{
					commonName:   "kubectl",
					extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				}),
			},
			VerifiedChains: [][]*x509.Certificate{{serverCert}},
		})
		if err == nil {
			t.Fatal("VerifyConnection() error = nil, want rejection for missing URI SAN")
		}
	})

	t.Run("client role rejects peer certs", func(t *testing.T) {
		cfg, _ := NewServerTLSConfig(conf, tlsFiles, RoleClient)
		err := cfg.VerifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				newLeaf(t, certSpec{
					commonName:   "node-2",
					uriSANs:      []*url.URL{BuildURISAN("my-cluster", RolePeer, "node-2")},
					extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				}),
			},
			VerifiedChains: [][]*x509.Certificate{{serverCert}},
		})
		if err == nil {
			t.Fatal("VerifyConnection() error = nil, want rejection")
		}
	})

	t.Run("client role rejects wrong cluster", func(t *testing.T) {
		cfg, _ := NewServerTLSConfig(conf, tlsFiles, RoleClient)
		err := cfg.VerifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				newLeaf(t, certSpec{
					commonName:   "kubectl",
					uriSANs:      []*url.URL{BuildURISAN("other-cluster", RoleClient, "kubectl")},
					extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				}),
			},
			VerifiedChains: [][]*x509.Certificate{{serverCert}},
		})
		if err == nil {
			t.Fatal("VerifyConnection() error = nil, want rejection")
		}
	})

	t.Run("peer role accepts peer certs", func(t *testing.T) {
		cfg, _ := NewServerTLSConfig(conf, tlsFiles, RolePeer)
		err := cfg.VerifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				newLeaf(t, certSpec{
					commonName:   "node-2",
					uriSANs:      []*url.URL{BuildURISAN("my-cluster", RolePeer, "node-2")},
					extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				}),
			},
			VerifiedChains: [][]*x509.Certificate{{serverCert}},
		})
		if err != nil {
			t.Fatalf("VerifyConnection() error = %v", err)
		}
	})

	t.Run("peer role rejects client certs", func(t *testing.T) {
		cfg, _ := NewServerTLSConfig(conf, tlsFiles, RolePeer)
		err := cfg.VerifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				newLeaf(t, certSpec{
					commonName:   "kubectl",
					uriSANs:      []*url.URL{BuildURISAN("my-cluster", RoleClient, "kubectl")},
					extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				}),
			},
			VerifiedChains: [][]*x509.Certificate{{serverCert}},
		})
		if err == nil {
			t.Fatal("VerifyConnection() error = nil, want rejection")
		}
	})

	t.Run("peer role rejects invalid identity", func(t *testing.T) {
		cfg, _ := NewServerTLSConfig(conf, tlsFiles, RolePeer)
		err := cfg.VerifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				newLeaf(t, certSpec{
					commonName:   "Node-2",
					uriSANs:      []*url.URL{BuildURISAN("my-cluster", RolePeer, "Node-2")},
					extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				}),
			},
			VerifiedChains: [][]*x509.Certificate{{serverCert}},
		})
		if err == nil {
			t.Fatal("VerifyConnection() error = nil, want rejection")
		}
	})

	t.Run("rejects unverified chain", func(t *testing.T) {
		cfg, _ := NewServerTLSConfig(conf, tlsFiles, RoleClient)
		err := cfg.VerifyConnection(tls.ConnectionState{
			PeerCertificates: []*x509.Certificate{
				newLeaf(t, certSpec{
					commonName:   "kubectl",
					uriSANs:      []*url.URL{BuildURISAN("my-cluster", RoleClient, "kubectl")},
					extKeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
				}),
			},
		})
		if err == nil {
			t.Fatal("VerifyConnection() error = nil, want rejection")
		}
	})
}

type certSpec struct {
	commonName   string
	uriSANs      []*url.URL
	dnsNames     []string
	ipAddrs      []net.IP
	extKeyUsages []x509.ExtKeyUsage
}

func newLeaf(t *testing.T, spec certSpec) *x509.Certificate {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() error = %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject: pkix.Name{
			CommonName: spec.commonName,
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           spec.extKeyUsages,
		BasicConstraintsValid: true,
		DNSNames:              spec.dnsNames,
		IPAddresses:           spec.ipAddrs,
		URIs:                  spec.uriSANs,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("x509.CreateCertificate() error = %v", err)
	}

	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("x509.ParseCertificate() error = %v", err)
	}

	return leaf
}
