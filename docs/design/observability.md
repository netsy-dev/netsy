---
title: "Observability"
weight: 90
description: "Metrics, structured logging, and debugging for Netsy clusters"
---

# Observability

Netsy exposes Prometheus-compatible metrics and structured logs for monitoring cluster health, debugging failures, and alerting on degradation.

The observability design is intended to explain:

- the Node's current role and state
- if the Primary is using `sync` or `quorum` writes
- whether Replicas are healthy enough for quorum
- if startup, catchup, draining, or election has stalled
- whether object storage, replication, or compaction is the bottleneck

## Metrics

Naming and instrumentation rules:

- All metrics use the `netsy_` prefix.
- Use base units in metric names e.g. durations use `_seconds`, sizes use `_bytes`.
- Use counters for cumulative events, gauges for current state, histograms for latency/size distributions.
- Keep labels bounded and low-cardinality. Avoid labels containing keys, revisions, object names, addresses, or error strings.
- Role-specific metrics use `netsy_primary_`, `netsy_elector_`, or `netsy_replica_` prefixes and are exposed via a custom `prometheus.Collector` that only emits them while the Node is currently in that role.
- When a Node leaves a role, role-specific metrics disappear from scrape output. They are not set to `0`.
- Do not rely on gauges for short-lived workflows that may complete between scrapes. Prefer counters, histograms, and structured logs for loading stages, preflight work, elections, and flushes.
- Standard gRPC server interceptor metrics (e.g. `grpc_server_handled_total`, `grpc_server_handling_seconds_bucket`) should be registered via [go-grpc-middleware](https://github.com/grpc-ecosystem/go-grpc-middleware) or equivalent for per-RPC observability across both Client and Peer gRPC servers. These complement the Netsy-specific metrics and are not documented individually here.

### Node State

Each state metric is a gauge vector with a `state` label. Exactly one label value is `1` at any time and all others are `0`.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `netsy_state_health` | Gauge | `state` | Current Health State. Values: `loading`, `healthy`, `degraded`. |
| `netsy_state_elector` | Gauge | `state` | Current Elector State. Values: `follower`, `leader`. |
| `netsy_state_primary` | Gauge | `state` | Current Primary State. Values: `replica`, `starting`, `active`, `draining`. |
| `netsy_process_start_time_seconds` | Gauge | | Unix timestamp when this Netsy process started. |
| `netsy_info` | Gauge | `version`, `cluster_id`, `node_id`, `quorum_config` | Build and configuration info. Always `1`. Useful for join-enriching dashboards and filtering by cluster or quorum mode. |

### Revisions

| Metric | Type | Description |
|---|---|---|
| `netsy_latest_revision` | Gauge | Highest revision present in this Node's local SQLite database. |
| `netsy_committed_revision` | Gauge | Current `committed_revision` on this Node. |
| `netsy_compaction_revision` | Gauge | Latest accepted compaction revision on this Node. |
| `netsy_primary_object_storage_revision` | Gauge | Highest revision known by the current Primary to be durably written to object storage. |

### Startup, Catchup, and Draining

These metrics make long-running phases visible beyond the high-level state gauges.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `netsy_loading_stage_duration_seconds` | Histogram | `stage`, `result` | Duration of individual loading stages. |
| `netsy_loading_restarts_total` | Counter | `reason` | Number of times the loading flow restarts. |
| `netsy_local_db_rebuilds_total` | Counter | `reason` | Number of times a Node discards or rebuilds local database state and starts over from snapshot, chunks, or a fresh schema. |
| `netsy_primary_preflight_stage_duration_seconds` | Histogram | `stage`, `result` | Duration of Primary preflight stages. |
| `netsy_primary_drain_duration_seconds` | Histogram | `result` | Time spent draining before stepping down or exiting. |
| `netsy_primary_chunk_buffer_flushes_total` | Counter | `trigger`, `result` | Chunk buffer flush attempts. `trigger` values: `size`, `age`, `draining`, `rollback`, `manual`. |
| `netsy_primary_chunk_buffer_flush_duration_seconds` | Histogram | `trigger`, `result` | Chunk buffer flush duration. |

### Client API (Read and Write Proxy)

These metrics cover the Client API surface, including range (read) requests served directly by any Node and write requests proxied by Replicas to the Primary.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `netsy_client_requests_total` | Counter | `kind`, `result` | Client API requests handled by this Node. `kind` is `range`, `txn`, `put`, `delete`, `compaction`, or `lease`. `result` is `success` or `error`. |
| `netsy_client_request_duration_seconds` | Histogram | `kind` | Client API request duration. |
| `netsy_replica_proxy_requests_total` | Counter | `kind`, `result` | Write requests proxied by this Replica to the Primary. |
| `netsy_replica_proxy_request_duration_seconds` | Histogram | `kind` | Duration of proxied write requests, measured from the Replica's perspective (includes network round-trip to Primary). |

### Write Path

The Primary chooses between synchronous object storage writes and quorum writes based on Replica health and quorum configuration.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `netsy_primary_write_path` | Gauge | `path` | Current write path. Exactly one label value is `1`. Values: `sync`, `quorum`. |
| `netsy_primary_quorum_rollbacks_total` | Counter | `reason` | Number of quorum transaction rollbacks. Reasons include `receipt_timeout` and `insufficient_receipts`. |
| `netsy_primary_write_transactions_total` | Counter | `path`, `result` | Total write transactions attempted by the Primary. |
| `netsy_primary_write_duration_seconds` | Histogram | `path`, `result` | End-to-end write transaction duration. |
| `netsy_primary_required_receipts` | Gauge | | Current Replica Receipt threshold required for quorum writes. |
| `netsy_primary_healthy_replicas` | Gauge | | Number of Replicas currently counted as healthy for quorum by the Primary. |
| `netsy_primary_receipted_replicas` | Gauge | | Number of Replicas that have successfully receipted at least once and are therefore eligible to count toward quorum. |

### Replica Health and Replication

These metrics describe the Primary's view of Replica health and quorum eligibility for write decisions.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `netsy_replica_receipt_age_seconds` | Gauge | | Seconds since this Node last successfully sent a Receipt to the Primary, computed at collect-time. |
| `netsy_primary_replication_streams` | Gauge | | Number of currently connected replication streams. |

### Elector Cluster View and Elections

These metrics describe the Elector's view of registered and healthy Nodes, and the results of Primary election attempts.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `netsy_elector_registered_nodes` | Gauge | | Number of currently registered Nodes in the Elector's in-memory map. |
| `netsy_elector_healthy_nodes` | Gauge | | Number of Nodes currently in `Healthy` Health State according to the Elector. |
| `netsy_elector_degraded_nodes` | Gauge | | Number of Nodes currently in `Degraded` Health State according to the Elector. |
| `netsy_elector_primary_elections_total` | Counter | `result` | Primary elections run by this Node as the Elector. Values: `success`, `failure`. |
| `netsy_elector_primary_election_failures_total` | Counter | `reason` | Failed Primary elections by failure reason. |
| `netsy_elector_primary_election_duration_seconds` | Histogram | `result` | End-to-end Primary election duration. |
| `netsy_elector_primary_election_contacts_total` | Counter | `result` | Node contact attempts made by the Elector during Primary elections. Values: `success`, `failure`. |

### Chunk Buffer

| Metric | Type | Description |
|---|---|---|
| `netsy_primary_chunk_buffer_records` | Gauge | Number of records currently in the Chunk Buffer. |
| `netsy_primary_chunk_buffer_bytes` | Gauge | Size in bytes of all records currently in the Chunk Buffer. |
| `netsy_primary_chunk_buffer_age_seconds` | Gauge | Age in seconds of the oldest unflushed record in the Chunk Buffer, computed at collect-time. `0` when buffer is empty. |

### Object Storage

Write metrics carry `kind` (`chunk` or `snapshot`) and `mode` (`sync` or `async`) labels so operators can distinguish client-facing sync writes from background buffer flushes. Read metrics carry only `result` — reads are off the hot path (bootstrap, preflight, discovery). Only explicitly instrumented write paths record metrics; internal metadata writes (discovery, registration) are excluded.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `netsy_object_storage_writes_total` | Counter | `kind`, `mode`, `result` | Object storage write attempts. `kind` is `chunk` or `snapshot`. `mode` is `sync` or `async`. |
| `netsy_object_storage_write_duration_seconds` | Histogram | `kind`, `mode`, `result` | Object storage write duration. |
| `netsy_object_storage_write_bytes` | Histogram | `kind`, `mode` | Payload size written to object storage. |
| `netsy_object_storage_reads_total` | Counter | `result` | Object storage read attempts. |
| `netsy_object_storage_read_duration_seconds` | Histogram | `result` | Object storage read duration. |

### Snapshots

Snapshot creation is a Primary-only maintenance operation that compacts Chunk files into a single Snapshot file. These metrics are separate from the general object storage write metrics to give visibility into snapshot scheduling and lifecycle.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `netsy_primary_snapshot_creations_total` | Counter | `result` | Snapshot creation attempts by the Primary. |
| `netsy_primary_snapshot_creation_duration_seconds` | Histogram | `result` | End-to-end duration of snapshot creation, including reading records from SQLite and uploading to object storage. |
| `netsy_primary_snapshot_age_seconds` | Gauge | | Seconds since the last successful snapshot was created, computed at collect-time. Useful for alerting when snapshots are not being created on schedule. |

After a Snapshot is uploaded, the Chunk files it covers are deleted by a separate background cleaner, in rate-limited batches. Cleanup never goes past the ceiling revision: the highest revision at or below which no further Chunk file can be written. On the quorum write path, records covered by a Snapshot may still be in the Chunk Buffer, so cleanup of those Chunk files waits for the buffer to flush.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `netsy_primary_snapshot_cleanup_runs_total` | Counter | `result` | Chunk cleanup runs. `result` is `success`, `partial` (some deletes failed and will be retried), `error` (listing failed), or `interrupted` (shutdown). |
| `netsy_primary_snapshot_cleanup_duration_seconds` | Histogram | `result` | End-to-end duration of a chunk cleanup run. |
| `netsy_primary_snapshot_cleanup_chunks_listed_total` | Counter | | Chunk objects found by listing for cleanup. |
| `netsy_primary_snapshot_cleanup_chunks_requested_total` | Counter | | Chunk keys sent for deletion in `direct` mode, without confirming they existed. |
| `netsy_primary_snapshot_cleanup_chunks_deleted_total` | Counter | | Chunk objects deleted after listing. |
| `netsy_primary_snapshot_cleanup_chunks_failed_total` | Counter | | Chunk object deletions that failed. |
| `netsy_primary_snapshot_cleanup_queued_revision` | Gauge | | Highest Snapshot revision queued for cleanup. |
| `netsy_primary_snapshot_cleanup_last_revision` | Gauge | | Highest revision fully cleaned. |
| `netsy_primary_snapshot_cleanup_ceiling_revision` | Gauge | | Highest revision cleanup may reach, because every Chunk file at or below it is already in object storage. |

### Retries

Every retry path has a corresponding counter so operators can see degradation before it becomes a failure.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `netsy_retries_total` | Counter | `operation` | Retry attempts by operation. Values include `object_storage_write`, `heartbeat_send`, `receipt_send`, `compaction_confirmation`, `election_contact`, `node_registration`. |

### Service Discovery and Registration

| Metric | Type | Labels | Description |
|---|---|---|---|
| `netsy_node_registration_duration_seconds` | Histogram | `result` | Duration of a Node's registration attempt with the Elector during loading. |
| `netsy_elector_auto_deregistrations_total` | Counter | | Number of Nodes automatically deregistered by the Elector after exceeding `elector.deregistration_timeout`. |

### Watches and Compaction

Note: `netsy_watch_min_revision` is emitted per Node. Detecting compaction-blocking skew requires comparing this metric across Prometheus `instance` labels (e.g. `min(netsy_watch_min_revision) by (instance)`).

| Metric | Type | Labels | Description |
|---|---|---|---|
| `netsy_watchers` | Gauge | | Number of connected Watchers on this Node. |
| `netsy_watches` | Gauge | | Number of active Watches on this Node. |
| `netsy_watch_min_revision` | Gauge | | Minimum revision across active Watches on this Node. If there are no active Watches, this equals `netsy_committed_revision`. |
| `netsy_compaction_duration_seconds` | Histogram | `result` | Duration of local compaction work on an individual Node after a compaction revision has been accepted. |
| `netsy_primary_compactions_total` | Counter | `result` | Compaction coordination runs initiated by the current Primary. |
| `netsy_primary_compaction_coordination_duration_seconds` | Histogram | `result` | Duration of a cluster-wide compaction coordination run on the Primary. |
| `netsy_primary_compaction_confirmation_failures_total` | Counter | `reason` | Failed compaction confirmations by reason. |

## Structured Logging

Netsy uses structured logs for all operationally significant events. Each log entry includes at minimum:

- `msg`
- `cluster_id`
- `node_id`
- timestamp

Logs should use stable event names and stable key names. Put detailed error text in `error`. Put short, bounded failure categories in `reason` so logs can be aggregated and alerted on safely.

### State and Lifecycle Events

Logged whenever a Node changes state or enters or exits a major lifecycle phase.

| Key | Description |
|---|---|
| `msg` | `state_transition`, `loading_stage_started`, `loading_stage_completed`, `loading_restarted`, `primary_preflight_stage_started`, `primary_preflight_stage_completed`, `drain_started`, `drain_completed` |
| `state_type` | `health`, `elector`, or `primary` for `state_transition` |
| `previous` | Previous state value |
| `new` | New state value |
| `stage` | Current loading, preflight, or drain stage |
| `reason` | Bounded trigger or failure reason |
| `duration_ms` | Stage duration when completed |
| `error` | Optional error text |

When local database state is newly created, discarded, or rebuilt, Netsy should emit dedicated lifecycle logs such as `local_db_initialized`, `local_db_rebuild_started`, and `local_db_rebuild_completed` with a bounded `reason` field.

### Election Events

Logged when an election starts, advances, completes, or fails.

| Key | Description |
|---|---|
| `msg` | `election_started`, `election_stage_completed`, `election_completed`, or `election_failed` |
| `role` | `elector` or `primary` |
| `stage` | Election stage name |
| `elected_node_id` | The Node elected on completion |
| `reason` | Failure reason on failure |
| `registered_nodes` | Number of registered Nodes |
| `contacted_nodes` | Number of Nodes successfully contacted |
| `healthy_candidates` | Number of healthy candidate Replicas considered |
| `duration_ms` | Election duration |

### Write Path and Transaction Events

Logged when the Primary switches write mode or when a write fails in an operationally interesting way.

| Key | Description |
|---|---|
| `msg` | `write_path_switched`, `write_transaction` (debug level), `quorum_rollback`, `write_failed`, `chunk_buffer_flush_started`, `chunk_buffer_flush_completed` |
| `path` | Current write path |
| `from` | Previous write path on switch |
| `to` | New write path on switch |
| `reason` | Bounded trigger or failure reason |
| `required_receipts` | Quorum threshold at the time |
| `received_receipts` | Number of Receipts received for the transaction |
| `healthy_replicas` | Primary's healthy Replica count |
| `revision` | Assigned revision for `write_transaction` |
| `trigger` | Chunk buffer flush trigger |
| `duration_ms` | Flush or write duration |
| `error` | Optional error text |

The `write_transaction` message is logged at debug level for every completed write transaction on the Primary. It includes `path`, `revision`, `healthy_replicas`, `required_receipts`, `received_receipts` (quorum only), and `duration_ms`. This is intentionally debug-level to avoid noise during normal operation, but is invaluable for diagnosing why individual transactions chose a particular write path.

### Registration Events

Logged by the Elector when Node registration changes.

| Key | Description |
|---|---|
| `msg` | `node_registered` or `node_deregistered` |
| `target_node_id` | The Node that registered or deregistered |
| `member_id` | The stable etcd `member_id` assigned or re-used |
| `trigger` | `startup`, `direct`, or `auto` |
| `reason` | Bounded trigger for deregistration (e.g. `timeout`, `shutdown`, `manual`) |
| `duration_ms` | Registration duration for `node_registered` |
| `error` | Optional error text on registration failure |

### Object Storage and Compaction Events

| Key | Description |
|---|---|
| `msg` | `object_storage_write`, `compaction_started`, `compaction_notice_failed`, `compaction_completed` |
| `kind` | `chunk` or `snapshot` |
| `mode` | `sync`, `async`, or `maintenance` |
| `revision` | Relevant revision for the operation |
| `compaction_revision` | Compaction revision when relevant |
| `reason` | Bounded failure reason |
| `duration_ms` | Operation duration |
| `error` | Optional error text |

## Debugging

### Key Relationships

When diagnosing issues, the following metric relationships are useful:

- __Quorum eligibility__: compare `netsy_primary_healthy_replicas` and `netsy_primary_required_receipts`. If healthy Replicas fall below the required threshold, `netsy_primary_write_path{path="sync"}` becomes `1`.
- __Replica-specific degradation__: compare `netsy_primary_healthy_replicas`, `netsy_primary_required_receipts`, `netsy_primary_replication_streams`, and each Replica's own `netsy_replica_receipt_age_seconds`. Use structured logs to identify which specific Replica is timing out, disconnected, or no longer counted toward quorum.
- __Replication lag__: compare `netsy_latest_revision` and `netsy_committed_revision` across Nodes to identify Replicas that are behind the current committed point.
- __Object storage lag__: on the Primary, compare `netsy_latest_revision` and `netsy_primary_object_storage_revision` to measure how far async object storage writes are behind quorum-committed data.
- __Buffer pressure__: rising `netsy_primary_chunk_buffer_bytes` together with rising `netsy_primary_chunk_buffer_age_seconds` suggests async flushes are not keeping up.
- __Retry pressure__: rising `netsy_retries_total` for `operation="object_storage_write"`, `operation="heartbeat_send"`, or `operation="receipt_send"` indicates degradation in progress. A sustained increase in object storage write retries may precede the Primary entering `Draining`.
- __Loading stalls__: if `netsy_state_health{state="loading"}` remains `1`, inspect recent `loading_stage_started` and `loading_stage_completed` logs together with `netsy_loading_stage_duration_seconds` to see which startup step is slow or failing.
- __Local DB rebuild churn__: increases in `netsy_local_db_rebuilds_total` indicate repeated local-database resets or rebuilds. Correlate with `local_db_rebuild_started` and `local_db_rebuild_completed` logs to see the trigger.
- __Election stalls__: inspect `netsy_elector_primary_election_duration_seconds`, `netsy_elector_primary_election_failures_total`, `netsy_elector_primary_election_contacts_total`, and election logs to determine whether the cluster is blocked on prior-Primary contact, node contactability, or candidate validation.
- __Compaction stalls__: rising `netsy_watch_min_revision` skew across Nodes (compare by Prometheus `instance` label), long `netsy_compaction_duration_seconds`, or repeated increments in `netsy_primary_compaction_confirmation_failures_total` indicate watch-admission, confirmation, or local compaction-work issues.
- __Snapshot staleness__: a rising `netsy_primary_snapshot_age_seconds` or repeated `netsy_primary_snapshot_creations_total{result="error"}` increments indicate snapshots are not being created, which increases loading/recovery time for new Nodes.
- __Snapshot cleanup backlog__: a sustained gap between `netsy_primary_snapshot_cleanup_queued_revision` and `netsy_primary_snapshot_cleanup_last_revision` means cleanup is behind. If `netsy_primary_snapshot_cleanup_ceiling_revision` is the value holding it back, cleanup is waiting for the Chunk Buffer to flush, which is expected for up to `replication.chunk_buffer.threshold_age_minutes`. Rising `netsy_primary_snapshot_cleanup_chunks_failed_total` or `netsy_primary_snapshot_cleanup_runs_total{result="partial"}` indicates delete failures; failed runs are retried after one minute.
- __Registration issues__: rising `netsy_retries_total{operation="node_registration"}` or long `netsy_node_registration_duration_seconds` during loading indicate Service Discovery or Elector connectivity problems. Correlate with `node_registered` / `node_deregistered` logs.
- __Client API health__: rising `netsy_client_requests_total{result="error"}` or elevated `netsy_client_request_duration_seconds` indicates client-facing degradation. Compare `netsy_replica_proxy_requests_total` error rates on Replicas to determine whether failures originate from the Replica's proxy path or the Primary itself.

### Alerting Notes

- Alerts should be written with Prometheus staleness semantics in mind. Role-specific metrics disappear when a Node is no longer the Primary or Elector.
- Prefer alerting on sustained conditions using `for:` rather than single scrape failures.
- Prefer counters for rate-based alerts and gauges for current-role or current-health dashboards.
