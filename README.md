# Netsy

[Netsy](https://netsy.dev) is a replicated key-value database which stores data in object storage. It implements a subset of the [etcd](https://etcd.io/) API (Range, Txn, Watch) and can be used as a drop-in etcd replacement for Kubernetes or as a general-purpose KV store for applications that need durable, low-latency persistence without the operational complexity of running a traditional database.

Unlike etcd which uses the Raft consensus algorithm, Netsy's multi-node replication is inspired by PostgreSQL synchronous streaming replication, and by modern architectures of systems like Loki/Mimir and OpenObserve which use object storage for data persistence.

Netsy is an Open Source project, created by [Nadrama](https://nadrama.com).

## Goals

Netsy was created to reduce operational complexity by providing durable, replicated key-value storage with the cost and reliability advantages of object storage, without the latency trade-offs.

* Object storage MUST be the permanent data store.

* Data MUST be durably stored — either synchronously in object storage, or via quorum-based replication — before being acknowledged

* Netsy MUST maintain compatibility with the subset of the etcd API used by Kubernetes.

* It is a non-goal to fully support the entire etcd API.

## Project Status

Netsy supports multi-node clusters with quorum-based replication, automatic leader election via s3lect, and graceful failover.

Current Features:

* KV ranges, KV transactions, Watches - supporting all options used by Kubernetes  
* Snapshots - full copies of records from leader SQLite database stored in object storage  
* Chunking - delta of records from last snapshot stored in object storage chunk files  
* Backfill from object storage - starting a fresh server restores from snapshots+chunks  
* Compaction - cluster-wide protocol to remove values from historical revisions

Roadmap:

* Conformance Tests - VCR-style acceptance test suite (in development)  
* Encryption - supporting value field encryption, with rolling key rotation  
* Leases - used by Kubernetes.

## Documentation

Check out the comprehensive documentation in [./docs](./docs) or read it rendered on the official Netsy website at [netsy.dev](https://netsy.dev)

## Development

See [docs/development.md](docs/development.md) for development environment setup and usage.

## License

Netsy is licensed under the Apache License, Version 2.0.
Copyright The Netsy Authors.

See the [LICENSE](./LICENSE) file for details.
