// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

type TLSFiles struct {
	ServerCert *tls.Certificate
	ServerCA   *x509.CertPool
	ClientCert *tls.Certificate
	ClientCA   *x509.CertPool
}

// LoadTLSFiles loads the configured CA and certificate/key pairs and parses their leaf certificates for later validation.
func LoadTLSFiles(c *Config) (*TLSFiles, error) {
	// Load server and client certificates and keys
	serverCert, err := tls.LoadX509KeyPair(c.TLSServerCert, c.TLSServerKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load server cert %s and key %s: %w", c.TLSServerCert, c.TLSServerKey, err)
	}
	if len(serverCert.Certificate) == 0 {
		return nil, fmt.Errorf("server certificate chain is empty")
	}
	serverCert.Leaf, err = x509.ParseCertificate(serverCert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse server certificate %s: %w", c.TLSServerCert, err)
	}
	clientCert, err := tls.LoadX509KeyPair(c.TLSClientCert, c.TLSClientKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load client cert %s and key %s: %w", c.TLSClientCert, c.TLSClientKey, err)
	}
	if len(clientCert.Certificate) == 0 {
		return nil, fmt.Errorf("client certificate chain is empty")
	}
	clientCert.Leaf, err = x509.ParseCertificate(clientCert.Certificate[0])
	if err != nil {
		return nil, fmt.Errorf("failed to parse client certificate %s: %w", c.TLSClientCert, err)
	}

	// Load single CA pool used for both server and client
	caPool := x509.NewCertPool()
	caPem, err := os.ReadFile(c.TLSCACert)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA file: %w", err)
	}
	if !caPool.AppendCertsFromPEM(caPem) {
		return nil, fmt.Errorf("failed to append CA cert to pool")
	}

	return &TLSFiles{
		ServerCA:   caPool,
		ServerCert: &serverCert,
		ClientCA:   caPool,
		ClientCert: &clientCert,
	}, nil
}
