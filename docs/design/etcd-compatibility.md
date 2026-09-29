---
title: "Compatibility With etcd"
weight: 80
description: "Netsy etcd API compatibility matrix and notes on supported and unsupported RPCs"
---

# Compatibility With etcd

Netsy intentionally implements a subset of the etcd API surface.

The compatibility target is the set of RPCs needed by core consumers such as `kube-apiserver`, plus a small number of operational RPCs that are useful for debugging and introspection.

Unless otherwise noted below:

- Unsupported RPCs return gRPC `Unimplemented`
- Compatibility notes describe intentional behavioral differences from etcd rather than bugs

## Overview Of Differences

- Netsy does not use Raft. It uses a Primary/Replica model with a separate Elector role.
- Multi-node topology is service-discovery-driven rather than consensus-log-driven.
- Netsy only supports single-key mutations per `Txn` request.
- Cluster membership for etcd compatibility APIs is exposed through the Elector using `members.json` and active `nodes/` registrations.
- Status information is local to the responding Node, even in a multi-node cluster.

## Supported RPCs

| Service | RPC | Notes |
|---|---|---|
| KV | `Range` | Replicas may serve reads up to `committed_revision`; degraded Replicas may still serve stale reads. |
| KV | `RangeStream` | Same access rules as `Range`. Chunks are sized adaptively based on observed response bytes (etcd's approach); merging all chunks of a stream yields the same result as a single `Range` call, including `Header`, `Count`, and `More`, which are only set on the final chunk. Custom sort orders return `Unimplemented`; `Range`'s other unsupported options return the same errors here. |
| KV | `Txn` | This is the write path Netsy relies on. Replicas proxy writes to the Primary and quorum/object-storage commit rules apply. |
| Watch | `Watch` | Watch event delivery is gated by `committed_revision`, and watch admission is constrained by compaction state. |
| Lease | `LeaseGrant` | `LeaseGrant` returns a synthetic response but does not implement real lease storage or expiry semantics. It should not be treated as etcd-compatible lease behavior. |
| Cluster | `MemberList` | Non-Elector Nodes proxy to the Elector, which answers from its in-memory Node map using stable etcd `member_id`s from `members.json`. |
| Maintenance | `Alarm` | Netsy does not implement real alarm semantics (NOSPACE, CORRUPT). When the node's Health State is `Healthy`, an empty alarm list is returned. When the node is `Loading` or `Degraded`, a `CORRUPT` alarm is returned so that `etcdctl endpoint health` correctly reports the node as unhealthy. |
| Maintenance | `Status` | `Status` is a local response: `leader` maps to the current Primary's stable etcd `member_id` from current Cluster State, `db_size`/`db_size_in_use` come from local SQLite, `errors` reflect local Health State, and Raft-only fields stay static. |

## Unsupported RPCs

| Service | RPC | Notes |
|---|---|---|
| KV | `Put` | Writes are exposed through `Txn` rather than standalone `Put`. |
| KV | `DeleteRange` | Deletes are exposed through `Txn` rather than standalone `DeleteRange`. |
| KV | `Compact` | Netsy has an internal compaction protocol, but the etcd `Compact` RPC currently remains intentionally unsupported. |
| Lease | `LeaseRevoke` | Returns `Unimplemented`. |
| Lease | `LeaseKeepAlive` | Returns a generic not-implemented error; intended behavior is unsupported. |
| Lease | `LeaseTimeToLive` | Returns `Unimplemented`. |
| Lease | `LeaseLeases` | Returns `Unimplemented`. |
| Cluster | `MemberAdd` | Netsy does not support explicit etcd membership-management RPCs. Cluster membership comes from Node registration plus Elector-managed `members.json`. |
| Cluster | `MemberRemove` | Same as above. |
| Cluster | `MemberUpdate` | Same as above. |
| Cluster | `MemberPromote` | Netsy has no learner role, so this remains unsupported. |
| Maintenance | `Defragment` | Returns `Unimplemented`. |
| Maintenance | `Hash` | Returns `Unimplemented`. |
| Maintenance | `HashKV` | Returns `Unimplemented`. |
| Maintenance | `Snapshot` | Returns `Unimplemented`. Netsy has its own object-storage snapshot mechanism rather than exposing etcd's maintenance snapshot RPC. |
| Maintenance | `MoveLeader` | Netsy has no Raft leader transfer RPC. Primary changes are handled through the Elector. |
| Maintenance | `Downgrade` | Returns `Unimplemented`. |
| Auth | `AuthEnable` | Netsy uses mTLS and does not implement etcd's Auth service. |
| Auth | `AuthDisable` | Same as above. |
| Auth | `AuthStatus` | Same as above. |
| Auth | `Authenticate` | Same as above. |
| Auth | `UserAdd` | Same as above. |
| Auth | `UserGet` | Same as above. |
| Auth | `UserList` | Same as above. |
| Auth | `UserDelete` | Same as above. |
| Auth | `UserChangePassword` | Same as above. |
| Auth | `UserGrantRole` | Same as above. |
| Auth | `UserRevokeRole` | Same as above. |
| Auth | `RoleAdd` | Same as above. |
| Auth | `RoleGet` | Same as above. |
| Auth | `RoleList` | Same as above. |
| Auth | `RoleDelete` | Same as above. |
| Auth | `RoleGrantPermission` | Same as above. |
| Auth | `RoleRevokePermission` | Same as above. |

## Detailed Differences

### `Txn` Supports Single-Key Operations Only

Netsy only supports single-key mutations per `Txn` request. Each `Txn` produces at most one KV event at one revision. Multi-key operations within a single `Txn` are not supported.

This is a deliberate simplification; Netsy assigns one revision per write operation and does not support the model used by etcd for multi-key transactions.

### Writes Use Primary/Elector Semantics, Not Raft

Netsy does not expose Raft semantics to clients. Instead:

- One Node is the `Primary` for writes.
- A separate `Elector` role manages Primary election.
- Majority quorum counts the Primary's own durable SQLite commit as one majority participant.

See [Storage & Replication](storage-replication.md) and [Leader Election](leader-election.md).

### `MemberList` Is Topology Compatibility, Not etcd Membership Management

Netsy's `MemberList` is an etcd-compatible view of current cluster topology.

- It is backed by the Elector's service-discovery state.
- Stable etcd `member_id` values are stored in `members.json` in object storage, managed by the Elector.
- Active Node registrations live under `nodes/` in object storage, managed by Nodes.
- Netsy does not implement `MemberAdd`, `MemberRemove`, `MemberUpdate`, or `MemberPromote` currently.

### `Status` Is Local, With Raft Fields Intentionally Static

`Status` is answered by the responding Node itself.

- `leader` is the current Primary's stable etcd `member_id` from current Cluster State.
- `db_size` and `db_size_in_use` come from the responding Node's local SQLite database.
- `errors` reflect the responding Node's local Health State.
- `raft_index`, `raft_term`, and `raft_applied_index` stay `0`.
- `is_learner` stays `false`.

Those Raft fields remain static because Netsy does not use Raft and has no learner role.

### Lease Support Is Not etcd-Compatible

Although `LeaseGrant` returns a response, Netsy currently does not implement etcd-compatible lease storage, keepalive, TTL lookup, or key-expiry behavior. Lease-dependent clients should treat lease support as unavailable.

### `Compact` RPC Is Separate From Netsy Compaction Design

Netsy includes its own compaction protocol, but that does not imply support for the etcd `KV/Compact` RPC. The RPC remains unsupported even though internal compaction exists.
