// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadTLSFilesParsesLeafCertificates(t *testing.T) {
	dir := t.TempDir()

	caKey, caCert := newCertificateAuthority(t, "my-cluster")
	serverKeyPath, serverCertPath := writeLeafPair(t, dir, caKey, caCert, pkix.Name{
		CommonName:         "node-1",
		Organization:       []string{"my-cluster"},
		OrganizationalUnit: []string{"peer"},
	}, []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth})
	clientKeyPath, clientCertPath := writeLeafPair(t, dir, caKey, caCert, pkix.Name{
		CommonName:         "node-1",
		Organization:       []string{"my-cluster"},
		OrganizationalUnit: []string{"peer"},
	}, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth})

	caPath := filepath.Join(dir, "ca.crt")
	writePEMFile(t, caPath, "CERTIFICATE", caCert.Raw)

	files, err := LoadTLSFiles(&Config{
		NodeConfig: NodeConfig{
			TLSCACert:     caPath,
			TLSServerCert: serverCertPath,
			TLSServerKey:  serverKeyPath,
			TLSClientCert: clientCertPath,
			TLSClientKey:  clientKeyPath,
		},
	})
	if err != nil {
		t.Fatalf("LoadTLSFiles() error = %v", err)
	}

	if files.ServerCert.Leaf == nil {
		t.Fatal("LoadTLSFiles() server leaf is nil")
	}
	if files.ClientCert.Leaf == nil {
		t.Fatal("LoadTLSFiles() client leaf is nil")
	}
	if got, want := files.ServerCert.Leaf.Subject.CommonName, "node-1"; got != want {
		t.Fatalf("server leaf common name = %q, want %q", got, want)
	}
	if got, want := files.ClientCert.Leaf.Subject.CommonName, "node-1"; got != want {
		t.Fatalf("client leaf common name = %q, want %q", got, want)
	}
}

func newCertificateAuthority(t *testing.T, clusterID string) (*rsa.PrivateKey, *x509.Certificate) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() error = %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          serialNumber(t),
		Subject:               pkix.Name{CommonName: clusterID + "-ca", Organization: []string{clusterID}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("x509.CreateCertificate() error = %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("x509.ParseCertificate() error = %v", err)
	}

	return key, cert
}

func writeLeafPair(t *testing.T, dir string, caKey *rsa.PrivateKey, caCert *x509.Certificate, subject pkix.Name, extKeyUsages []x509.ExtKeyUsage) (string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey() error = %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          serialNumber(t),
		Subject:               subject,
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           extKeyUsages,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("x509.CreateCertificate() error = %v", err)
	}

	keyPath := filepath.Join(dir, subject.CommonName+"-"+extUsageName(extKeyUsages[0])+".key")
	certPath := filepath.Join(dir, subject.CommonName+"-"+extUsageName(extKeyUsages[0])+".crt")
	writePEMFile(t, keyPath, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key))
	writePEMFile(t, certPath, "CERTIFICATE", der)

	return keyPath, certPath
}

func writePEMFile(t *testing.T, path, pemType string, der []byte) {
	t.Helper()

	data := pem.EncodeToMemory(&pem.Block{Type: pemType, Bytes: der})
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", path, err)
	}
}

func serialNumber(t *testing.T) *big.Int {
	t.Helper()

	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		t.Fatalf("rand.Int() error = %v", err)
	}
	return n
}

func extUsageName(usage x509.ExtKeyUsage) string {
	switch usage {
	case x509.ExtKeyUsageServerAuth:
		return "server"
	case x509.ExtKeyUsageClientAuth:
		return "client"
	default:
		return "unknown"
	}
}
