#!/usr/bin/env bash
# Netsy <https://netsy.dev>
# Copyright The Netsy Authors
# SPDX-License-Identifier: Apache-2.0

# Dev server wrapper for running Netsy under Overmind.
# Supports single-instance (hot reload via Air) and multi-instance (direct binary) modes.
# Called by overmind via the Procfile — use 'make start' to start.
set -eo pipefail

CURRENT=$(dirname "$(readlink -f "$0")")
ROOT="${CURRENT}/../.."

# ---------------------------------------------------------------------------
# Instance number from Overmind's PS variable
# ---------------------------------------------------------------------------
# Overmind sets PS to "netsy" (unscaled) or "netsy#1", "netsy#2", ... (scaled).
if [ -z "${PS:-}" ]; then
    echo "ERROR: PS environment variable not set. This script must be run under Overmind."
    echo "Use 'make start' to start the development environment."
    exit 1
fi
PROC_NUM="${PS##*#}"        # "netsy#2" → "2"
# If PS was "netsy" (no #), the ## strip is a no-op and PROC_NUM="netsy".
# Detect that and default to 1.
if ! [[ "${PROC_NUM}" =~ ^[0-9]+$ ]]; then
    PROC_NUM=1
fi

# ---------------------------------------------------------------------------
# Port numbering approach: base + (N-1) * PORT_STEP
# ---------------------------------------------------------------------------
PORT_STEP=10
OFFSET=$(( (PROC_NUM - 1) * PORT_STEP ))

CLIENT_PORT=$(( 2378 + OFFSET ))
PEER_PORT=$(( 2381 + OFFSET ))
ELECTION_PORT=$(( 18443 + OFFSET ))
HEALTH_PORT=$(( 18080 + OFFSET ))

# ---------------------------------------------------------------------------
# AWS S3 (localstack-compatible dev-s3 server)
# ---------------------------------------------------------------------------
export AWS_S3_USE_PATH_STYLE=true
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1
export AWS_ENDPOINT_URL=http://localhost:4566

# ---------------------------------------------------------------------------
# Netsy per-instance settings
# ---------------------------------------------------------------------------
export NETSY_CONFIG="${ROOT}/examples/config.jsonc"
export NETSY_NODE_ID="dev-node-${PROC_NUM}"
export NETSY_DATA_DIR="${ROOT}/temp/data-${PROC_NUM}"
export NETSY_BIND_CLIENT="127.0.0.1:${CLIENT_PORT}"
export NETSY_BIND_PEER="127.0.0.1:${PEER_PORT}"
export NETSY_BIND_ELECTION="127.0.0.1:${ELECTION_PORT}"
export NETSY_BIND_HEALTH="127.0.0.1:${HEALTH_PORT}"
export NETSY_ADVERTISE_CLIENT="127.0.0.1:${CLIENT_PORT}"
export NETSY_ADVERTISE_PEER="127.0.0.1:${PEER_PORT}"
export NETSY_ADVERTISE_ELECTION="127.0.0.1:${ELECTION_PORT}"

# ---------------------------------------------------------------------------
# TLS certificates (per-instance server/peer, shared CA and client)
# ---------------------------------------------------------------------------
export NETSY_TLS_CA_CERT="${ROOT}/temp/certs/ca.crt"
export NETSY_TLS_SERVER_CERT="${ROOT}/temp/certs/dev-node-${PROC_NUM}/server.crt"
export NETSY_TLS_SERVER_KEY="${ROOT}/temp/certs/dev-node-${PROC_NUM}/server.key"
export NETSY_TLS_CLIENT_CERT="${ROOT}/temp/certs/dev-node-${PROC_NUM}/peer.crt"
export NETSY_TLS_CLIENT_KEY="${ROOT}/temp/certs/dev-node-${PROC_NUM}/peer.key"

# ---------------------------------------------------------------------------
# Per-instance log file
# ---------------------------------------------------------------------------
LOG_FILE="${ROOT}/temp/logs/netsy-${PROC_NUM}.log"
mkdir -p "${ROOT}/temp/data-${PROC_NUM}" "${ROOT}/temp/logs"

# ---------------------------------------------------------------------------
# Startup banner
# ---------------------------------------------------------------------------
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  Netsy instance ${PROC_NUM}                                          ║"
echo "╠══════════════════════════════════════════════════════════════╣"
printf "║  node_id:    %-47s ║\n" "${NETSY_NODE_ID}"
printf "║  client:     %-47s ║\n" "127.0.0.1:${CLIENT_PORT}"
printf "║  peer:       %-47s ║\n" "127.0.0.1:${PEER_PORT}"
printf "║  election:   %-47s ║\n" "127.0.0.1:${ELECTION_PORT}"
printf "║  health:     %-47s ║\n" "127.0.0.1:${HEALTH_PORT}"
printf "║  data_dir:   %-47s ║\n" "temp/data-${PROC_NUM}"
printf "║  log_file:   %-47s ║\n" "temp/logs/netsy-${PROC_NUM}.log"
echo "╚══════════════════════════════════════════════════════════════╝"

# ---------------------------------------------------------------------------
# Determine instance count from OVERMIND_FORMATION
# ---------------------------------------------------------------------------
INSTANCE_COUNT=1
if [ -n "${OVERMIND_FORMATION:-}" ]; then
    # Extract netsy=N from the formation string
    if [[ "${OVERMIND_FORMATION}" =~ netsy=([0-9]+) ]]; then
        INSTANCE_COUNT="${BASH_REMATCH[1]}"
    fi
fi

# ---------------------------------------------------------------------------
# Runtime mode: Air (single-instance default) or direct-run loop
# ---------------------------------------------------------------------------
if [ "${INSTANCE_COUNT}" -le 1 ] && [ "${NETSY_DEV_AIR:-1}" != "0" ]; then
    # Single-instance: use Air for hot reload
    exec air -c "${ROOT}/scripts/dev/.air.toml" 2>&1 | tee "${LOG_FILE}"
else
    # Run the binary directly in a restart loop. This is the default for
    # multi-instance dev clusters, and can be forced for a single instance
    # with NETSY_DEV_AIR=0 make start.
    # Use 'make build' before starting, and 'make restart' to pick up changes.
    BINARY="${ROOT}/bin/netsy"
    if [ ! -f "${BINARY}" ]; then
        echo "Binary not found at ${BINARY} — run 'make build' first."
        exit 1
    fi

    # Stagger non-primary instances so the elector's peer gRPC server
    # is ready before other nodes attempt registration.
    if [ "${PROC_NUM}" -gt 1 ]; then
        sleep 1
    fi

    while true; do
        echo "[instance ${PROC_NUM}] Starting ${BINARY}..."
        "${BINARY}" 2>&1 | tee -a "${LOG_FILE}" || true
        echo "[instance ${PROC_NUM}] Process exited, restarting in 2s..."
        sleep 2
    done
fi
