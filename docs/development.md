---
title: "Development"
weight: 40
description: "How to work on and develop Netsy locally"
---

# Developing Netsy

## Prerequisites

Install the following tools:

- [Go](https://go.dev/dl/) (see `go.mod` for required version)
- [Air](https://github.com/air-verse/air) — live reload (`go install github.com/air-verse/air@latest`)
- [Overmind](https://github.com/DarthSim/overmind) — process manager (`brew install overmind`)

Run `make setup` to verify all required tools are installed.

## Quick Start

Start the full development environment (fake S3 server + Netsy with live reload):

```
make start
```

This will:
1. Generate development TLS certificates in `temp/certs/` if they don't exist
2. Check that all required ports are available
3. Start the fake S3 server (`cmd/dev-s3`) on `:4566`
4. Start Netsy via Air for live reload

Cluster config is loaded from `examples/config.jsonc`.

## Multi-Instance Dev

Run multiple Netsy instances to test clustering, replication, and leader
election locally:

```
make start NETSY_COUNT=3
```

This will:
1. Generate per-instance TLS certificates for `dev-node-1` through `dev-node-3`
2. Build the `netsy` binary
3. Check that all required ports are available
4. Start 3 Netsy instances under Overmind

Each instance gets a unique node ID, data directory, port set, log file, and
TLS certificates. Instance 1 keeps the standard development ports so existing
helper scripts continue to work unchanged.

### Port Scheme

Ports use a fixed step of 10 per instance:

| Port      | Instance 1 | Instance 2 | Instance 3 |
|-----------|-----------|-----------|-----------|
| Client    | 2378      | 2388      | 2398      |
| Peer      | 2381      | 2391      | 2401      |
| Election  | 18443     | 18453     | 18463     |
| Health    | 18080     | 18090     | 18100     |

Formula: `port = base + (N - 1) * 10`

### Iterating in Multi-Instance Mode

Multi-instance mode does not use Air for hot reload. Instead, rebuild and
restart:

```
make build restart
```

This rebuilds the binary and restarts all Netsy instances managed by Overmind.

### Cluster Formation

Netsy uses object-storage-based service discovery. Each node registers itself
by writing to `nodes/{node_id}.json` in S3, and the Elector bootstraps
membership by scanning the `nodes/` prefix. There is no static peer list.

This means multi-node local dev forms a cluster automatically, as long as
each instance has a unique `node_id`, unique advertise addresses, and matching
TLS certs, they will discover each other through the shared dev S3 bucket.
No peer configuration is needed.

## Debug Logging

Enable debug-level logging with the `NETSY_DEBUG` environment variable:

```
NETSY_DEBUG=true make start
```

This works with both single and multi-instance modes. Debug logging is verbose
and includes every heartbeat tick, stream message, and degradation check.

## Viewing Logs

Logs are written to `temp/logs/`:
- `temp/logs/dev-s3.log` — fake S3 server
- `temp/logs/netsy-1.log` — Netsy instance 1
- `temp/logs/netsy-2.log` — Netsy instance 2 (scaled mode)
- etc.

To view logs from a specific process in real-time:

```
overmind echo netsy
overmind echo s3
```

## TLS Certificates

Development certificates are generated automatically by `make start`. The
certificate layout:

```
temp/certs/
├── ca.crt                      # Shared dev CA
├── ca.key
├── client.crt                  # Shared external client cert (etcdctl, kube-apiserver)
├── client.key
├── service-account.key         # Kubernetes service account signing key
├── dev-node-1/
│   ├── server.crt / .key       # Instance 1 server cert (URI SAN: netsy://dev-cluster/peer/dev-node-1)
│   └── peer.crt / .key         # Instance 1 peer client cert
├── dev-node-2/
│   ├── server.crt / .key
│   └── peer.crt / .key
└── ...
```

Generate certs manually:

```
./scripts/dev/certs.sh        # 1 instance (default)
./scripts/dev/certs.sh 3      # 3 instances
```

Regenerate all certificates:

```
rm -rf temp/certs/ && ./scripts/dev/certs.sh 3
```

## Resetting Dev Data

Reset everything (certs, database, S3 data, logs):

```
make stop && make start
```

Reset only the database (instance 1):

```
rm -f temp/data-1/db.sqlite3*
```

Reset only S3 data:

```
rm -rf temp/dev-s3/
```

## Testing with kube-apiserver

Run a kube-apiserver container configured to use Netsy as its etcd backend:

```
./scripts/kubernetes/kube-apiserver.sh
```

Press `Ctrl+C` to stop. Requires a running Netsy instance (`make start`).
Helper scripts use instance 1 ports by default.

## etcdctl

Run `etcdctl` with the correct certs and endpoint:

```
./scripts/etcdctl/etcdctl.sh endpoint status
./scripts/etcdctl/etcdctl.sh get "" --prefix
```

Helper scripts use instance 1 ports by default.

## Git Hooks

Install the pre-commit hook:

```
make setup
```
