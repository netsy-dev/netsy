---
title: "Configuration"
weight: 10
description: "Netsy configuration reference — environment variables and cluster config file"
---

# Configuration

Netsy configuration is split into three tiers:

- **Per-node settings** are unique to each Node (identity, addresses, TLS cert paths). Set via environment variables only.
- **Per-cluster settings** are identical across all Nodes (cluster behaviour, storage, thresholds). Set via a shared config file, pointed at by the `NETSY_CONFIG` env var or `--config` flag.
- **Object storage connectivity** may additionally be configured via SDK-specific environment variables. These follow the standard conventions of the cloud provider SDK.

### Validating a Config File

Use `--validate` to check a config file without starting the server:

```bash
netsy --validate /etc/netsy/config.jsonc
```

Exits with code `0` and a success message if valid, or code `1` with error details if invalid.

## Validation Rules

- Both `cluster_id` and `node_id` must be lowercase alphanumeric characters and hyphens only, with no leading, trailing, or consecutive hyphens, and a maximum of 32 characters.
- `elector.degradation_count` must be >= 1
- `replication.degradation_count` must be >= 1
- `elector.primary_prior_timeout` must be >= `elector.degradation_count` × `heartbeat_interval` — the Elector must not give up waiting for the previous Primary before it would even be marked Degraded
- `replication.chunk_buffer.threshold_age_minutes` must be > 0 when `replication.quorum` is not `0` — without a time-based flush, a low-write cluster could hold unflushed data in memory indefinitely

## Per-Node Settings (environment variables)

Unique to each Node — identity, network addresses, TLS certificate paths, and data directory.

| Env Var | Default | Description |
|---|---|---|
| `NETSY_CONFIG` | — | Path to cluster config file (required unless `--config` is specified) |
| `NETSY_NODE_ID` | — | Node identifier (must match CN in TLS certificates) |
| `NETSY_BIND_CLIENT` | `:2378` | Client API bind address |
| `NETSY_ADVERTISE_CLIENT` | — | Client API advertise address |
| `NETSY_BIND_PEER` | `:2381` | Peer API bind address |
| `NETSY_ADVERTISE_PEER` | — | Peer API advertise address |
| `NETSY_BIND_ELECTION` | `:8443` | s3lect health server bind address |
| `NETSY_ADVERTISE_ELECTION` | — | s3lect health server advertise address |
| `NETSY_BIND_HEALTH` | `:8080` | HTTP health endpoint bind address |
| `NETSY_TLS_CA_CERT` | — | CA certificate path (single CA for all cluster TLS certs) |
| `NETSY_TLS_SERVER_CERT` | — | Server TLS certificate path |
| `NETSY_TLS_SERVER_KEY` | — | Server TLS private key path |
| `NETSY_TLS_CLIENT_CERT` | — | Client TLS certificate path |
| `NETSY_TLS_CLIENT_KEY` | — | Client TLS private key path |
| `NETSY_DATA_DIR` | `/var/lib/netsy` | Data directory path |
| `NETSY_DEBUG` | `false` | Enable verbose output |

## Per-Cluster Settings (config file)

Identical across all Nodes — cluster identity, storage, behaviour, and thresholds. Set in a shared config file only.

### Per-Node Address Example

Use advertise addresses that other Nodes and Clients can actually dial.

IPv4 example:

```bash
export NETSY_ADVERTISE_CLIENT="172.16.0.1:2378"
export NETSY_ADVERTISE_PEER="172.16.0.1:2381"
export NETSY_ADVERTISE_ELECTION="172.16.0.1:8443"
```

IPv6 example:

```bash
export NETSY_ADVERTISE_CLIENT="[2001:db8::10]:2378"
export NETSY_ADVERTISE_PEER="[2001:db8::10]:2381"
export NETSY_ADVERTISE_ELECTION="[2001:db8::10]:8443"
```

### Config File Format

The config file uses [JSONC](https://jsonc.org) (JSON with Comments). Point Netsy at it with:

```bash
export NETSY_CONFIG=/etc/netsy/config.jsonc
# or
netsy --config /etc/netsy/config.jsonc
```

### Config File Example

```jsonc
{
  // Cluster identity — must match the Organization (O) field in TLS certificates
  "cluster_id": "my-cluster",

  // Object storage configuration
  "storage": {
    "provider": "s3",       // "s3" or "gcs"
    "bucket_name": "my-netsy-bucket",
    "key_prefix": "",       // optional prefix for all object storage keys
    "class": "STANDARD",    // S3: STANDARD/STANDARD_IA/... ; GCS: STANDARD/NEARLINE/COLDLINE/ARCHIVE
    "encryption": "provider-managed" // or "customer-managed"
    // "kms_key_id": ""     // only needed when using customer-managed encryption
  },

  // How often each Node sends heartbeats to the Elector and Primary
  "heartbeat_interval": "1s",

  // Elector leader election and node lifecycle
  "elector": {
    "degradation_count": 2,           // number of missed heartbeats before Node is marked Degraded
    "deregistration_timeout": "3m",   // auto-deregister Degraded nodes after this duration ("0" to disable)
    "primary_prior_timeout": "5s"     // how long to wait for the previous Primary during election before proceeding
  },

  // Replication stream between Primary and Replicas
  "replication": {
    "quorum": -1,                     // -1 = majority, 0 = disabled (sync to object storage), positive int = static
    "degradation_count": 2,           // number of missed Heartbeats/Receipts before Replica is excluded from quorum
    // Buffer for async object storage writes
    "chunk_buffer": {
      "threshold_size_mb": 4,         // flush chunk buffer to object storage when it exceeds this size
      "threshold_age_minutes": 1      // flush chunk buffer to object storage after this duration
    }
  },

  // Snapshot creation thresholds
  "snapshot": {
    "threshold_records": 10000,       // create snapshot after N records since last snapshot
    "threshold_size_mb": 10000,       // create snapshot when chunks exceed N MB
    "threshold_age_minutes": 0,       // create snapshot after N minutes since last snapshot (0 = disabled)
    "cleanup_batch_size": 250,        // chunk objects deleted per cleanup batch after a snapshot
    "cleanup_batch_interval": "1s",   // pause between cleanup batches
    "cleanup_strategy": "auto"        // how cleanup finds chunks to delete: auto, list, or direct
  },

  // How often the Primary schedules compaction across all Nodes
  "compaction_interval": "5m"
}
```

GCS example:

```jsonc
{
  "cluster_id": "my-cluster",
  "storage": {
    "provider": "gcs",
    "bucket_name": "my-netsy-bucket",
    "key_prefix": "",
    "class": "STANDARD",
    "encryption": "customer-managed",
    "kms_key_id": "projects/my-project/locations/global/keyRings/netsy/cryptoKeys/main"
  }
}
```

### Config File Reference

| Config Key | Default | Description |
|---|---|---|
| `cluster_id` | — | Cluster identifier; validated against TLS cert Organization (O) field |
| `storage.provider` | `s3` | Object storage provider (`s3` or `gcs`) |
| `storage.bucket_name` | — | Object storage bucket name |
| `storage.key_prefix` | — | Object storage key prefix |
| `storage.class` | `STANDARD` | Provider-specific storage class |
| `storage.encryption` | `provider-managed` | Encryption mode: `provider-managed` or `customer-managed` |
| `storage.kms_key_id` | — | KMS key identifier/resource when using `customer-managed` encryption |
| `heartbeat_interval` | — | How often each Node sends heartbeats to the Elector and Primary |
| `elector.degradation_count` | `2` | Number of consecutive missed heartbeats before Node is marked Degraded |
| `elector.deregistration_timeout` | `3m` | Auto-deregister Degraded nodes (`0` = disabled) |
| `elector.primary_prior_timeout` | — | Timeout for contacting previous Primary during election |
| `replication.quorum` | `-1` | Quorum config: `-1` (majority), `0` (disabled), positive int (static) |
| `replication.degradation_count` | `2` | Number of consecutive missed Heartbeats/Receipts before Replica is excluded from quorum; the Primary may also mark a Replica `Degraded` immediately on quorum receipt timeout |
| `replication.chunk_buffer.threshold_size_mb` | — | Chunk Buffer size-based flush threshold |
| `replication.chunk_buffer.threshold_age_minutes` | — | Chunk Buffer time-based flush threshold |
| `snapshot.threshold_records` | `10000` | Snapshot after N records |
| `snapshot.threshold_size_mb` | `10000` | Snapshot when chunks exceed N MB |
| `snapshot.threshold_age_minutes` | `0` | Snapshot after N minutes (`0` = disabled) |
| `snapshot.cleanup_batch_size` | `250` | Chunk objects deleted per batch when cleaning up Chunk files covered by a Snapshot |
| `snapshot.cleanup_batch_interval` | `1s` | Pause between cleanup batches, to keep deletes from competing with Chunk writes |
| `snapshot.cleanup_strategy` | `auto` | `list` lists Chunk files before deleting them; `direct` deletes computed Chunk keys without listing; `auto` uses `direct` when the backend supports bulk delete, else `list`. Use `list` on versioned buckets, where deleting a missing key still creates a delete marker |
| `compaction_interval` | — | Compaction scheduling interval |

For replication, the heartbeat-based degradation window is `heartbeat_interval × replication.degradation_count`. If that window is longer than the quorum receipt timeout, a Replica may be marked `Degraded` immediately on quorum timeout and then recover quickly on a subsequent Heartbeat or Receipt.

### Object Storage Connectivity (env vars)

Additional to the cluster-wide object storage configuration, provider SDK environment variables may be used.

#### S3 / AWS

| Env Var | Default | Description |
|---|---|---|
| `AWS_ENDPOINT_URL` | — | Custom S3/storage endpoint URL |
| `AWS_ACCESS_KEY_ID` | — | AWS access key ID |
| `AWS_SECRET_ACCESS_KEY` | — | AWS secret access key |
| `AWS_SESSION_TOKEN` | — | AWS session token |
| `AWS_DEFAULT_REGION` | `us-east-1` | AWS region |
| `AWS_ROLE_ARN` | — | IAM role ARN to assume |
| `AWS_ROLE_SESSION_NAME` | `netsy-session` | Session name for IAM role |
| `AWS_S3_USE_PATH_STYLE` | `false` | Path-style S3 addressing (for MinIO) |

#### GCS / GCP

Prefer Application Default Credentials (ADC). On GCE/GKE this usually means using the attached service account or workload identity. Outside GCP, point the SDK at a service-account JSON file.

| Env Var | Default | Description |
|---|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | — | Path to a GCP service-account JSON credentials file |

### Provider Notes

- For `storage.provider = "s3"`, `storage.class` uses S3 storage class names and `storage.kms_key_id` should be an AWS KMS key ID or ARN.
- For `storage.provider = "gcs"`, `storage.class` uses GCS storage classes such as `STANDARD`, `NEARLINE`, `COLDLINE`, or `ARCHIVE`, and `storage.kms_key_id` should be a full Cloud KMS key resource.
- `storage.encryption = "provider-managed"` uses the cloud provider's default server-side encryption.
- `storage.encryption = "customer-managed"` requires `storage.kms_key_id`.
