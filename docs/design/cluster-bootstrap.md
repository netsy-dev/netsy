---
title: "Cluster State Bootstrap"
weight: 35
description: "Seeding a new Netsy cluster from an operator-provided bootstrap.netsy snapshot"
---

# Cluster State Bootstrap

Netsy can initialise a brand-new cluster from a fixed object storage file:

```text
bootstrap.netsy
```

This file is an operator-provided Netsy snapshot file. It is only used for initialising the state of a previously uninitialised Netsy cluster.

## When Netsy Uses It

Netsy only considers `bootstrap.netsy` when the Elector cannot find `members.json` in object storage. That is the existing marker for an uninitialised cluster.

If `members.json` already exists, Netsy ignores `bootstrap.netsy`.

## Promotion Before Cluster Initialisation

`bootstrap.netsy` is not imported directly into a node's local SQLite database. During first Elector bootstrap, Netsy:

1. reads `bootstrap.netsy` if present;
2. validates it as a `KIND_SNAPSHOT` Netsy data file;
3. verifies every record and the file footer CRCs;
4. requires records to be contiguous from revision `1` through the file's last revision;
5. refuses to continue if normal durable history already exists under `chunks/` or under a different `snapshots/<revision>.netsy` key;
6. creates the normal durable snapshot key using the last revision from the bootstrap snapshot:

   ```text
   snapshots/<zero-padded-last-revision>.netsy
   ```

7. only after that snapshot exists, writes `members.json`.

After `members.json` exists, normal node bootstrap discovers the promoted snapshot through the standard `snapshots/` path and imports it like any other Netsy-created snapshot.

This happens in the Elector bootstrap path, rather than Primary startup, because `members.json` is the durable cluster-initialisation marker and the first Primary is not elected until Elector bootstrap has completed. This gives crash-safe behaviour:

- crash before promotion: no `members.json`; the next Elector retries from `bootstrap.netsy`;
- crash after promotion but before `members.json`: no `members.json`; the next Elector retries promotion idempotently if the promoted snapshot has identical contents;
- crash after `members.json`: the cluster is initialised; future starts ignore `bootstrap.netsy` and use normal `snapshots/` and `chunks/` loading.

## Operator Contract

Place `bootstrap.netsy` in object storage before starting a brand-new cluster. Netsy only reads it while `members.json` is absent; after `members.json` exists, `bootstrap.netsy` is ignored.

Use it only in an otherwise empty Netsy object storage namespace. Do not place it alongside existing `members.json`, `chunks/`, or normal `snapshots/` history. If `bootstrap.netsy` is present during first-cluster bootstrap (while `members.json` does not exist) and conflicting durable history exists, Netsy fails loudly rather than merging histories.

The `bootstrap.netsy` file must be a valid Netsy snapshot data file with contiguous revisions starting at `1`.
