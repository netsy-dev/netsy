---
title: "System Design"
weight: 30
description: "Netsy concepts and system design for data files, multi-node/replication, and watches/compaction"
---

## In This Section

- [Overview](overview.md) – Overview of Netsy terminology, requirements, leader election, and startup process.
- [Leader Election](leader-election.md) - Netsy two-tier leader election system design.
- [Netsy Data Files](data-files.md) – Netsy (.netsy) data file format/specification.
- [Cluster State Bootstrap](cluster-bootstrap.md) – Seeding a new cluster from a bootstrap snapshot file.
- [Storage & Replication](storage-replication.md) – Netsy data storage and replication system design.
- [Loading, Startup, & Shutdown](loading-startup.md) - Outline of how Node Loading, Primary Startup, and graceful Node Shutdown works.
- [Failure Scenarios](failure-scenarios.md) – Data integrity and safety analysis across quorum configurations and cluster sizes.
- [Watches & Compaction](watches-compaction.md) – Watch support & Compaction system design.
- [Compatibility With etcd](etcd-compatibility.md) – Supported etcd RPCs, unsupported RPCs, and notes on compatibility differences.
- [Observability](observability.md) – Metrics, structured logging, and debugging for Netsy clusters.
