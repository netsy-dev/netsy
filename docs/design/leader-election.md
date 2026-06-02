---
title: "Leader Election"
weight: 20
description: "Netsy two-tier leader election system design"
---

# Leader Election for Elector & Primary Roles

Unlike etcd, Netsy does not use Raft or consensus algorithms. Netsy is more like a traditional database such as PostgreSQL with its synchronous streaming replication.

Netsy only ever has a single Primary Node which is responsible for handling all etcd transactions (write operations) in a Cluster, and it serializes all writes.

The Primary Node is determined through a leader election process, to which its correctness is critical to data consistency and integrity.

## Prerequisites for Replication

Due to the replication model used by Netsy, there is a strict requirement when electing a new Primary: Netsy must audit enough Nodes in the cluster to safely determine which has the latest known revision, to prevent data loss. The number of Nodes the Elector must contact during leader election depends on the [Quorum Configuration](storage-replication.md#quorum-configuration):

- __Disabled quorum__ (`0`): the Elector does not need to contact other Nodes, since all writes are synchronous to object storage and no Node can hold un-synced data. The Elector can elect from Nodes it already knows about via recent Heartbeat data, selecting the one with the highest known revision.
- __Static quorum__ (positive integer): the Elector must contact all registered Nodes. Since a static quorum may be less than a majority, a committed record could exist on a minority of Replicas. The Elector must verify all Nodes to find the one with the highest revision. Leader election will block until all Nodes are contactable, or until unavailable Nodes are deregistered.
- __Majority quorum__ (`-1`): the Elector only needs to contact a majority of registered Nodes (`floor(N/2) + 1` where N is the total number of registered Nodes). This is safe because majority quorum guarantees that any committed write was receipted by a majority, and any other majority must overlap with at least one Node that has that write.

While etcd handles this using raft, Netsy does not use consensus: instead, it uses a two-tier leader election system, allowing the audit process to more efficiently run on a single Node known as the Elector.

## Two-Tier Leader Election

Why are there two tiers? The “second tier” leader election logic for determining the Primary, including the latest-revision audit, is implemented in a Netsy process itself - but which Node is responsible for running this is determined through the “first tier” leader election logic, which uses s3lect to coordinate leader election using object storage, and gives the node responsible the role of the Elector.

Nodes can establish which Node is currently the Elector using s3lect, which does so by querying object storage, and also has an efficient HTTPS-based mechanism during a stable state.

Nodes can determine which Node is the Primary by querying the Elector's `GetClusterState` RPC, which returns the authoritative current Elector / Primary role info (Node and Member IDs and Peer advertise addresses).

## Service Discovery for Nodes

The Elector requires knowledge of all Nodes in its Cluster. The mechanism for registering and listing Nodes is referred to as Service Discovery.

During the Node `Loading` Health State, Netsy ensures a JSON file is written to object storage for its Node ID, containing its Advertise address(es). The path used for this is `nodes/{node_id}.json`.

If a Node finds an existing registration file for its own `node_id` with the same data such as advertise addresses, it treats that as a no-op. If the existing file contains different advertise addresses, the Node fails startup rather than silently overwriting the existing registration for the same `node_id`.

Separately, the Elector manages a durable `members.json` file containing the `cluster_id` and stable etcd `member_id -> node_id` mappings. This file is updated only by the Elector using an object storage precondition (conditional-write / compare-and-swap semantics) during direct Node registration, allowing the same `node_id` to re-use its previous `member_id` after deregistration or restart. The assigned or re-used `member_id` is returned to the Node in the registration response.

When a Node becomes an Elector, it stores a map of Nodes, their addresses, and their stable etcd `member_id` values in-memory, populated immediately from `members.json` and the active `nodes/` registration files in object storage, and updated by each new Node registration directly once the Node becomes Elector (which can happen before the object storage backfill is completed). If `members.json` does not exist yet, the first Elector creates it and allocates stable `member_id` values for the discovered active Nodes, including itself, before serving authoritative Cluster State. This in-memory map is the authoritative role-to-address source used by `GetClusterState`. Until the initial load from object storage is complete, an Elector will not perform leader election for a new Primary.
If an operator-provided `bootstrap.netsy` exists, the first Elector promotes it into normal snapshot history before creating `members.json`; see [Cluster State Bootstrap](cluster-bootstrap.md).

### Node Deregistration

When a Node is being removed from the Cluster (e.g. scaling down, maintenance, or graceful shutdown), it must deregister itself to prevent stale entries from blocking leader election:

1. If the Node is the Primary, it moves its Primary State to `Draining` and waits until all buffered records are flushed to object storage.
2. The Node moves its Health State to `Degraded`.
3. The Node deregisters with the current Elector directly (removing itself from the Elector's in-memory Node map).
4. The Node deletes its registration file from object storage (under the `nodes/` prefix). Its durable `member_id` mapping remains in `members.json` for future re-registration.

The Elector must also handle stale registrations: if a registered Node has missed Heartbeats and is marked `Degraded` by the Elector, it is excluded from leader election but remains in the Node map. If a Node remains Degraded for longer than the `elector.deregistration_timeout` (default: 3 minutes), the Elector automatically deregisters it by removing it from the in-memory Node map and deleting its registration file from object storage. Setting this to `0` disables automatic deregistration, requiring an operator to manually deregister unavailable Nodes. This is safe because a Degraded Node has already caused the Primary to potentially fall back to synchronous S3 writes, so no un-synced data can be lost. If the Node later recovers, it will re-register during its `Loading` process. This is particularly important for majority quorum (`-1`), where stale registrations inflate the registered Node count and raise the majority threshold unnecessarily.

## Heartbeat Mechanism

Each Node sends a Heartbeat to the Elector on a regular cadence. The Heartbeat contains the Node's current Health State, Primary State, and latest Revision. A Heartbeat is also embedded in every Receipt sent to the Primary, sharing the same message structure, so the server-side processing of a Receipt triggers the same code path as receiving a standalone Heartbeat.

Each Node is aware of both the current Elector and Primary Node IDs. The Node uses this to determine when to send Heartbeats:

- __To the Elector__: a Heartbeat is always sent on a regular cadence.
- __To the Primary__: a Heartbeat is only sent if no Receipt has been sent within the cadence/timeout, since every Receipt already contains a Heartbeat.
- __When the Elector and Primary are the same Node__: the Node only needs to send a single Heartbeat (or Receipt) to that Node. If Receipts are being sent frequently enough, no standalone Heartbeats are needed at all.

This design means the Elector and Primary use the same server-side code path for processing Heartbeats, whether they arrive as standalone Heartbeats or embedded in Receipts.

### Heartbeat-Based Degradation

The Elector and Primary both mark a Node as `Degraded` if it has missed 2 consecutive Heartbeats. Likewise, a Node must mark itself as `Degraded` if it has failed to successfully send a Receipt or a Heartbeat after 1 immediate retry.

The Primary may also mark a Replica `Degraded` immediately when it misses a quorum receipt deadline, since that Replica is not healthy enough to participate in quorum transactions at the configured latency budget.

This can also recover quickly: if the heartbeat-based degradation window is longer than the quorum timeout, the timeout-triggered `Degraded` state primarily acts as a short-lived fast path to force the next client retry onto synchronous object storage writes until the Replica proves it is healthy again via a subsequent Heartbeat or Receipt.

When the Primary marks a Node as `Degraded`, it will then stop counting that Replica as healthy for quorum, and if the healthy Replica count drops below the configured quorum threshold, the Primary falls back to synchronous object storage writes. This ensures that any committed writes are durably stored in S3 before the client receives a response, and no partitioned Replica can hold un-synced data that is not also in object storage.

## Elector Leader Election

If the Elector determines no Primary exists (either via a failed health check, or failed 'who is the Primary' request from Peers), the Elector begins the leader election process:

1. If a previous Primary is known, the Elector must attempt to contact it directly, regardless of its current Health State (even if Degraded). This gives the old Primary a chance to confirm it has finished Draining and moved to `Replica`, or to report that it is still `Active` or `Draining` (in which case leader election is deferred). If the previous Primary is unreachable, the Elector retries for a configurable timeout period before proceeding without it. This timeout balances giving the old Primary time to drain against the need to restore write availability to the Cluster.

2. Retrieve the current Health State, current Primary State, Start Time, and latest Revision from Nodes, including itself. The Elector already has recent Heartbeat data for each Node, but retrieves the latest state directly during election for accuracy. The number of Nodes that must be successfully contacted (excluding the previous Primary, which is handled in step 1) depends on the quorum configuration:

   - __Disabled quorum__ (`0`): no additional contactability requirement. The Elector uses its existing Heartbeat data to identify Healthy Nodes and their latest revisions. Since all writes are synchronous to S3, the Elector can safely elect from known Nodes without contacting them first.

   - __Static quorum__ (positive integer): all registered Nodes must be contacted. Since a static quorum may be less than a majority, the Elector cannot safely exclude any Node — a committed record may only exist on a minority of Replicas. Leader election will block until all Nodes respond, or until unavailable Nodes are deregistered.

   - __Majority quorum__ (`-1`): a majority of registered Nodes (`floor(N/2) + 1`) must be successfully contacted. Fail leader election if fewer than a majority respond. This is safe because any committed quorum write was receipted by a majority of Nodes, so any majority the Elector contacts must include at least one Node with the latest data.

3. Exit leader election and save the Primary to local memory if a single Node has the `Active` Primary State.

4. Fail leader election if any of the contacted Nodes has a Primary State that is not `Replica`.

   - This prevents a new Primary being elected while the existing Primary is `Starting`, `Active`, or `Draining`.

   - Degraded Nodes (other than the previous Primary, which is handled in step 1) are excluded from this check. Because a Node that cannot reach the Elector self-degrades and communicates this to the Primary (causing a fallback to synchronous S3 writes), a Degraded Node cannot hold un-synced data that is not already in object storage. This makes it safe to proceed with leader election without checking a Degraded Node's Primary State. Note that for __static quorum__, step 2 still requires all registered Nodes to be contactable — this exclusion only applies to the Primary State check, not the contactability requirement.

   - For planned Node removal, see [Node Deregistration](#node-deregistration).

5. Filter any Nodes not in the `Healthy` Health State. Fail leader election if the Nodes list is empty.

6. Sort remaining Nodes by the latest Revision from highest to lowest, then by the Node Start Time, and filter any below the largest Revision number.

7. Elects the first of the remaining Nodes which will have the highest Revision number and latest Start Time. That Node may be itself if it has an equal or highest Revision number.

  - The latest Start Time is picked as Nodes may be periodically rotated, and selecting the newest Node will presumably result in the longest leadership lifetime.

Once a Node has been elected as the Primary, no action is taken until the Cluster State is pushed to each Node.

Leader election will continue retrying every 500 milliseconds until successful, and if the Elector crashes mid-leader election of a new Primary it can safely retry.

## Node State Checks & Cluster State Push

Once an Elector is newly elected and has loaded its Service Discovery map, it can begin to push Cluster State to each Node, and receive Node State information via Heartbeats from each Node.

- Cluster State includes the current Elector and current Primary, expressed as NodeInfo (containing Node ID, stable etcd `member_id`, and Peer advertise address), along with the total registered node count used for majority quorum calculation.

- Node State is received by the Elector from each Node via Heartbeats (sent on a regular cadence, and embedded in every Receipt sent to the Primary). This includes the Node's current Health State, current Primary State, and latest Revision.

This approach is taken to propagate Cluster State as fast as possible.

- The current Elector is immediately pushed to each Node, rather than waiting on each Node to discover who the new Elector is using s3lect.

- Each Node can detect if there's a bug resulting in Elector split-brain because it will have two Electors attempting to connect concurrently and can guard against this.

- Non-Elector Nodes treat the Elector's `GetClusterState` response and `PushClusterState` updates as the authoritative mapping from the current Elector / Primary roles to dialable Peer addresses.

Cluster State push is triggered immediately as part of Netsy updating its in-memory state for Elector and Primary. When iterating over each Node to push this Cluster State, it will always push the latest Cluster State to the Primary first, followed by all other known Nodes.

- When a Node receives the updated Cluster State indicating it has become the Primary, it will move its Primary State from `Replica` to `Starting` and begin to perform pre-flight checks.

- When a Replica receives updated Cluster State indicating the Primary has changed, it immediately reconnects its replication stream to the new Primary using the pushed Peer advertise address.

## Primary Node

Once Replicas receives Cluster State indicating there is a new Primary elected, each Replica will immediately establish a new gRPC bidirectional stream connection to the new Primary.

This stream is used to:

1. Receive an `Initial` message carrying the current Committed and Compaction Revision, then receive new Records from the Primary
2. Send a Receipt confirming a new Record has been committed by the Replica (every Receipt embeds a Heartbeat message containing the Node's current state)
3. Send a standalone Heartbeat if no Receipt has been sent within the heartbeat cadence/timeout

Separately, the Primary sends a logical Commit message on the `Follow` stream to advance the Replica's `committed_revision` once a Record is considered committed, and sends a logical Compact message once a new Compaction Revision has been tenatively held cluster-wide. The initial message sent when the stream is established updates the local Committed Revision and Compaction Revision using the same code path as later Commit and Compact updates.

The Primary processes Receipt-embedded Heartbeats and standalone Heartbeats using the same code path, since both contain identical Node State information (Health State, Primary State, latest Revision).

More details about the protocol are covered under [Object Storage & Multi-Node Replication](storage-replication.md).
