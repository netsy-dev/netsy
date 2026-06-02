#!/bin/bash
# Netsy <https://netsy.dev>
# Copyright The Netsy Authors
# SPDX-License-Identifier: Apache-2.0

# Diagnostic script for the Netsy dev environment (make dev).
# Assumes `make dev` is running locally, which starts dev-s3 and
# netsy instance(s) via Overmind with Air hot-reload.
#
# Checks:
# 1. Overmind and dev processes are running
# 2. Health endpoints are reachable
# 3. Log files for errors/panics
# 4. Elector leadership and primary election status
# 5. Heartbeat health

ROOT_DIR="$(git rev-parse --show-toplevel)"
LOG_DIR="${ROOT_DIR}/temp/logs"

PASS_COUNT=0
WARN_COUNT=0
FAIL_COUNT=0

pass() { PASS_COUNT=$((PASS_COUNT + 1)); echo "  ✅ $1"; }
warn() { WARN_COUNT=$((WARN_COUNT + 1)); echo "  ⚠️  $1"; }
fail() { FAIL_COUNT=$((FAIL_COUNT + 1)); echo "  ❌ $1"; }
section() { echo ""; echo "━━━ $1 ━━━"; }

# Detect instance count from OVERMIND_FORMATION or running log files
detect_instances() {
    local count=0
    for f in "${LOG_DIR}"/netsy-*.log; do
        [ -f "$f" ] && count=$((count + 1))
    done
    [ "$count" -eq 0 ] && count=1
    echo "$count"
}

INSTANCE_COUNT=$(detect_instances)

# ============================================================================
# 1. Process Checks
# ============================================================================
section "Process Checks"

PROCS_OK=true

if [ -S "${ROOT_DIR}/.overmind.sock" ]; then
    pass "Overmind socket exists"
else
    fail "Overmind socket not found — is 'make dev' running?"
    PROCS_OK=false
fi

if pgrep -f "dev-s3" > /dev/null 2>&1; then
    pass "dev-s3 process running"
else
    fail "dev-s3 process not running"
    PROCS_OK=false
fi

if pgrep -f "bin/netsy" > /dev/null 2>&1; then
    pass "netsy process running"
else
    fail "netsy process not running"
    PROCS_OK=false
fi

if [ "$PROCS_OK" = false ]; then
    echo ""
    echo "  Dev environment is not running. Start it with 'make dev'."
    echo ""
    exit 1
fi

# Report restart count per instance
for i in $(seq 1 "$INSTANCE_COUNT"); do
    logfile="${LOG_DIR}/netsy-${i}.log"
    [ -f "$logfile" ] || continue
    starts=$(grep -c "starting health" "$logfile" 2>/dev/null || true)
    restarts=$(( starts - 1 ))
    if [ "$restarts" -le 0 ]; then
        pass "netsy-${i} — no restarts"
    else
        warn "netsy-${i} — $restarts restart(s)"
    fi
done

# ============================================================================
# 2. Health Endpoint Checks
# ============================================================================
section "Health Endpoints"

check_health() {
    local name="$1"
    local url="$2"
    local response
    response=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "$url" 2>/dev/null)
    if [ "$response" = "200" ]; then
        pass "$name — HTTP 200"
    else
        fail "$name — HTTP $response (expected 200)"
    fi
}

check_port() {
    local name="$1"
    local port="$2"
    if nc -z localhost "$port" 2>/dev/null; then
        pass "$name — port $port open"
    else
        fail "$name — port $port not reachable"
    fi
}

# dev-s3
check_port "dev-s3" 4566

# Per-instance checks
for i in $(seq 1 "$INSTANCE_COUNT"); do
    offset=$(( (i - 1) * 10 ))
    health_port=$(( 8080 + offset ))
    client_port=$(( 2378 + offset ))
    peer_port=$(( 2381 + offset ))

    check_health "netsy-${i} health" "http://localhost:${health_port}/health"
    check_port "netsy-${i} client gRPC" "$client_port"
    check_port "netsy-${i} peer gRPC" "$peer_port"
done

# ============================================================================
# 3. Log File Checks
# ============================================================================
section "Log Files"

check_log() {
    local name="$1"
    local logfile="$2"

    if [ ! -f "$logfile" ]; then
        fail "$name — log file not found ($logfile)"
        return
    fi

    local size
    size=$(wc -c < "$logfile" 2>/dev/null | tr -d ' ')
    if [ "$size" -eq 0 ]; then
        warn "$name — log file is empty"
        return
    fi

    local panics fatals errors warnings
    panics=$(grep -c -i "panic" "$logfile" 2>/dev/null || true)
    fatals=$(grep -c -i "fatal" "$logfile" 2>/dev/null || true)
    errors=$(grep -c "level=ERROR" "$logfile" 2>/dev/null || true)
    warnings=$(grep -c "level=WARN" "$logfile" 2>/dev/null || true)

    if [ "$panics" -gt 0 ]; then
        fail "$name — $panics panic(s) found"
        grep -i "panic" "$logfile" | tail -3 | sed 's/^/        /'
    elif [ "$fatals" -gt 0 ]; then
        fail "$name — $fatals fatal(s) found"
        grep -i "fatal" "$logfile" | tail -3 | sed 's/^/        /'
    elif [ "$errors" -gt 0 ]; then
        warn "$name — $errors error-level log(s) found"
        grep "level=ERROR" "$logfile" | tail -3 | sed 's/^/        /'
    elif [ "$warnings" -gt 0 ]; then
        warn "$name — $warnings warn-level log(s) found"
        grep "level=WARN" "$logfile" | tail -3 | sed 's/^/        /'
    else
        pass "$name — no panics, fatals, errors, or warnings"
    fi
}

check_log "dev-s3" "${LOG_DIR}/dev-s3.log"
for i in $(seq 1 "$INSTANCE_COUNT"); do
    check_log "netsy-${i}" "${LOG_DIR}/netsy-${i}.log"
done

# ============================================================================
# 4. Election Status
# ============================================================================
section "Election Status"

ELECTOR_FOUND=false
for i in $(seq 1 "$INSTANCE_COUNT"); do
    logfile="${LOG_DIR}/netsy-${i}.log"
    [ -f "$logfile" ] || continue

    # Elector leadership
    if grep -q "acquired elector leadership" "$logfile" 2>/dev/null; then
        pass "netsy-${i} — acquired elector leadership"
        ELECTOR_FOUND=true

        # Primary election (only the elector runs elections)
        if grep -q "election_completed" "$logfile" 2>/dev/null; then
            elected=$(grep "election_completed" "$logfile" | tail -1 | grep -o 'elected_node_id=[^ ]*' | cut -d= -f2)
            pass "netsy-${i} — primary elected (${elected})"
        else
            fail_count=$(grep -c "election_failed" "$logfile" 2>/dev/null || true)
            if [ "$fail_count" -gt 0 ]; then
                last_reason=$(grep "election_failed" "$logfile" | tail -1 | grep -o 'error="[^"]*"' | head -1)
                fail "netsy-${i} — no primary elected ($fail_count failed attempts, last: $last_reason)"
            else
                fail "netsy-${i} — elector has no election activity"
            fi
        fi
    fi
done
if [ "$ELECTOR_FOUND" = false ]; then
    fail "no node acquired elector leadership"
fi

# ============================================================================
# 5. Heartbeat Health
# ============================================================================
section "Heartbeat Health"

for i in $(seq 1 "$INSTANCE_COUNT"); do
    logfile="${LOG_DIR}/netsy-${i}.log"
    [ -f "$logfile" ] || continue

    degraded_count=$(grep -c "degraded" "$logfile" 2>/dev/null || true)
    hb_failures=$(grep "heartbeat" "$logfile" 2>/dev/null | grep -c -i "fail\|error" || true)
    self_degraded=$(grep -c "node self-degraded" "$logfile" 2>/dev/null || true)

    if [ "$self_degraded" -gt 0 ]; then
        # Check if it recovered (became healthy after self-degradation, e.g. after restart)
        last_health=$(grep "state_type=health" "$logfile" | tail -1 | grep -o 'new=[^ ]*' | cut -d= -f2)
        if [ "$last_health" = "healthy" ]; then
            warn "netsy-${i} — self-degraded $self_degraded time(s) but recovered to healthy"
        else
            fail "netsy-${i} — node self-degraded ($self_degraded time(s)), current state: $last_health"
        fi
    elif [ "$degraded_count" -gt 0 ]; then
        # Check if it recovered (became healthy after degradation)
        last_health=$(grep "state_type=health" "$logfile" | tail -1 | grep -o 'new=[^ ]*' | cut -d= -f2)
        if [ "$last_health" = "healthy" ]; then
            warn "netsy-${i} — was degraded $degraded_count time(s) but recovered to healthy"
        else
            fail "netsy-${i} — degraded $degraded_count time(s), current state: $last_health"
        fi
    elif [ "$hb_failures" -gt 0 ]; then
        warn "netsy-${i} — $hb_failures heartbeat failure(s) in logs"
    else
        pass "netsy-${i} — heartbeats healthy, no degradation"
    fi
done

# ============================================================================
# 6. Functional Test (write to each node, then read from each node)
# ============================================================================
section "Functional Test"

CERTS_DIR="${ROOT_DIR}/temp/certs"

if ! command -v etcdctl &>/dev/null; then
    warn "etcdctl not found — skipping functional test"
else
    ETCD_ARGS=(--cert="${CERTS_DIR}/client.crt" --key="${CERTS_DIR}/client.key" --cacert="${CERTS_DIR}/ca.crt" -w json)
    TEST_PREFIX="__netsy_check_dev_$$"
    WRITE_OK=true

    client_port_for() { echo $(( 2378 + ($1 - 1) * 10 )); }

    # Build comma-separated list of all endpoints for fallback routing
    ALL_ENDPOINTS=""
    for i in $(seq 1 "$INSTANCE_COUNT"); do
        [ -n "$ALL_ENDPOINTS" ] && ALL_ENDPOINTS="${ALL_ENDPOINTS},"
        ALL_ENDPOINTS="${ALL_ENDPOINTS}127.0.0.1:$(client_port_for "$i")"
    done

    declare -a WRITTEN_VALUES

    # Phase 1: Write a unique key via each node (tests write proxying on replicas)
    for i in $(seq 1 "$INSTANCE_COUNT"); do
        client_port=$(client_port_for "$i")
        test_key="${TEST_PREFIX}_${i}"
        test_value="check-dev-${i}-$(date +%s)"
        WRITTEN_VALUES[i]="$test_value"

        TXN="mod(\"${test_key}\") = \"0\"

put \"${test_key}\" \"${test_value}\"

"
        txn_out=$(echo "${TXN}" | etcdctl "${ETCD_ARGS[@]}" \
            --endpoints="127.0.0.1:${client_port}" txn 2>&1) || true

        if echo "$txn_out" | grep -q '"succeeded":true'; then
            pass "netsy-${i} write (:${client_port}) — succeeded"
        else
            fail "netsy-${i} write (:${client_port}) — failed"
            WRITE_OK=false
        fi
    done

    # Phase 2: Read all keys from every node (tests replication)
    if [ "$WRITE_OK" = true ]; then
        sleep 1
        for reader in $(seq 1 "$INSTANCE_COUNT"); do
            client_port=$(client_port_for "$reader")
            all_found=true

            for writer in $(seq 1 "$INSTANCE_COUNT"); do
                test_key="${TEST_PREFIX}_${writer}"
                expected_b64=$(echo -n "${WRITTEN_VALUES[$writer]}" | base64)
                range_out=$(etcdctl "${ETCD_ARGS[@]}" \
                    --endpoints="127.0.0.1:${client_port}" get "${test_key}" 2>&1) || true

                if ! echo "$range_out" | grep -q "${expected_b64}"; then
                    all_found=false
                    break
                fi
            done

            if [ "$all_found" = true ]; then
                pass "netsy-${reader} read  (:${client_port}) — all ${INSTANCE_COUNT} keys present"
            else
                fail "netsy-${reader} read  (:${client_port}) — missing key from netsy-${writer}"
            fi
        done
    fi

    # Cleanup
    for i in $(seq 1 "$INSTANCE_COUNT"); do
        test_key="${TEST_PREFIX}_${i}"
        etcdctl "${ETCD_ARGS[@]}" --endpoints="${ALL_ENDPOINTS}" del "${test_key}" &>/dev/null || true
    done
fi

# ============================================================================
# Summary
# ============================================================================
section "Summary"

echo ""
echo "  ✅ Passed: $PASS_COUNT"
echo "  ⚠️  Warnings: $WARN_COUNT"
echo "  ❌ Failed: $FAIL_COUNT"
echo ""

if [ "$FAIL_COUNT" -gt 0 ]; then
    exit 1
else
    exit 0
fi
