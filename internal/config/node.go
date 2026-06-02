// Netsy <https://netsy.dev>
// Copyright The Netsy Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"os"
	"path/filepath"
	"strings"
)

// NodeConfig holds per-node settings populated from environment variables.
type NodeConfig struct {
	ConfigPath        string // from NETSY_CONFIG env or --config flag
	NodeID            string // NETSY_NODE_ID
	BindClient        string // NETSY_BIND_CLIENT (default :2378)
	AdvertiseClient   string // NETSY_ADVERTISE_CLIENT
	BindPeer          string // NETSY_BIND_PEER (default :2381)
	AdvertisePeer     string // NETSY_ADVERTISE_PEER
	BindElection      string // NETSY_BIND_ELECTION (default :8443)
	AdvertiseElection string // NETSY_ADVERTISE_ELECTION
	BindHealth        string // NETSY_BIND_HEALTH (default :8080)
	TLSCACert         string // NETSY_TLS_CA_CERT
	TLSServerCert     string // NETSY_TLS_SERVER_CERT
	TLSServerKey      string // NETSY_TLS_SERVER_KEY
	TLSClientCert     string // NETSY_TLS_CLIENT_CERT
	TLSClientKey      string // NETSY_TLS_CLIENT_KEY
	DataDir           string // NETSY_DATA_DIR (default /var/lib/netsy)
	Verbose           bool   // NETSY_DEBUG
}

// LoadNodeConfig reads environment variables and applies defaults.
func LoadNodeConfig() NodeConfig {
	n := NodeConfig{
		ConfigPath:        envOrDefault("NETSY_CONFIG", ""),
		NodeID:            envOrDefault("NETSY_NODE_ID", ""),
		BindClient:        envOrDefault("NETSY_BIND_CLIENT", ":2378"),
		AdvertiseClient:   envOrDefault("NETSY_ADVERTISE_CLIENT", ""),
		BindPeer:          envOrDefault("NETSY_BIND_PEER", ":2381"),
		AdvertisePeer:     envOrDefault("NETSY_ADVERTISE_PEER", ""),
		BindElection:      envOrDefault("NETSY_BIND_ELECTION", ":8443"),
		AdvertiseElection: envOrDefault("NETSY_ADVERTISE_ELECTION", ""),
		BindHealth:        envOrDefault("NETSY_BIND_HEALTH", ":8080"),
		TLSCACert:         envOrDefault("NETSY_TLS_CA_CERT", ""),
		TLSServerCert:     envOrDefault("NETSY_TLS_SERVER_CERT", ""),
		TLSServerKey:      envOrDefault("NETSY_TLS_SERVER_KEY", ""),
		TLSClientCert:     envOrDefault("NETSY_TLS_CLIENT_CERT", ""),
		TLSClientKey:      envOrDefault("NETSY_TLS_CLIENT_KEY", ""),
		DataDir:           envOrDefault("NETSY_DATA_DIR", "/var/lib/netsy"),
		Verbose:           envOrDefault("NETSY_DEBUG", "false") == "true",
	}

	// Resolve relative paths
	n.TLSCACert = resolvePath(n.TLSCACert)
	n.TLSServerCert = resolvePath(n.TLSServerCert)
	n.TLSServerKey = resolvePath(n.TLSServerKey)
	n.TLSClientCert = resolvePath(n.TLSClientCert)
	n.TLSClientKey = resolvePath(n.TLSClientKey)
	n.DataDir = resolvePath(n.DataDir)

	return n
}

func envOrDefault(key, defaultValue string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return defaultValue
}

func resolvePath(path string) string {
	if strings.HasPrefix(path, "./") {
		path = strings.TrimPrefix(path, "./")
		currentDir, _ := filepath.Abs(".")
		path = filepath.Join(currentDir, path)
	}
	return path
}
