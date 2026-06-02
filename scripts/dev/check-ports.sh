#!/usr/bin/env bash
# Netsy <https://netsy.dev>
# Copyright The Netsy Authors
# SPDX-License-Identifier: Apache-2.0

# Pre-flight port checker for Netsy dev environment.
# Computes the full set of required ports and fails early if any are in use.
#
# Usage:
#   ./scripts/check-ports.sh        # check ports for 1 instance
#   ./scripts/check-ports.sh 3      # check ports for 3 instances
set -eo pipefail

INSTANCE_COUNT="${1:-1}"
PORT_STEP=10
ERRORS=0

check_port() {
    local port=$1
    local label=$2
    if lsof -i ":${port}" -sTCP:LISTEN >/dev/null 2>&1; then
        echo "  ✗ Port ${port} (${label}) is already in use"
        pid=$(lsof -ti ":${port}" -sTCP:LISTEN 2>/dev/null || true)
        if [ -n "${pid}" ]; then
            proc=$(ps -p "${pid}" -o comm= 2>/dev/null || echo "unknown")
            echo "    → PID ${pid} (${proc})"
        fi
        ERRORS=$((ERRORS + 1))
    else
        echo "  ✓ Port ${port} (${label}) is available"
    fi
}

echo "Checking ports for ${INSTANCE_COUNT} Netsy instance(s)..."
echo ""

# Always check the dev S3 port
check_port 4566 "dev-s3"

for i in $(seq 1 "${INSTANCE_COUNT}"); do
    OFFSET=$(( (i - 1) * PORT_STEP ))
    echo ""
    echo "Instance ${i}:"
    check_port $(( 2378 + OFFSET )) "instance ${i} client"
    check_port $(( 2381 + OFFSET )) "instance ${i} peer"
    check_port $(( 8443 + OFFSET )) "instance ${i} election"
    check_port $(( 8080 + OFFSET )) "instance ${i} health"
done

echo ""
if [ "${ERRORS}" -gt 0 ]; then
    echo "✗ ${ERRORS} port(s) already in use. Free them before starting dev."
    echo ""
    echo "Tip: run 'make stop' to stop stale dev processes."
    exit 1
else
    echo "✓ All ports are available."
fi
