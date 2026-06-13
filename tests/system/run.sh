#!/usr/bin/env bash
set -uo pipefail

# System tests for svc-notifier.
# Requires: Postgres + NATS running (docker-compose.test.yml).
# Usage: ./tests/system/run.sh

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"

# --- Config ---
export DATABASE_URL_OWNER="postgres://owner:owner@localhost:5432/svc_notifier_test"
export DATABASE_URL="postgres://svc_notifier_app:svc_notifier_app@localhost:5432/svc_notifier_test"
export DATABASE_URL_INGEST="postgres://svc_notifier_ingest:svc_notifier_ingest@localhost:5432/svc_notifier_test"
export NATS_URL="nats://localhost:4222"
export PORT=8010

PASS=0
FAIL=0
SVC_PID=""
PORT_COUNTER=0

# --- Helpers ---
cleanup() {
    if [ -n "$SVC_PID" ] && kill -0 "$SVC_PID" 2>/dev/null; then
        kill "$SVC_PID" 2>/dev/null
        wait "$SVC_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT

log_pass() { PASS=$((PASS + 1)); echo "  PASS: $1"; }
log_fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1"; }

wait_for_health() {
    local url="$1"
    local retries=30
    for i in $(seq 1 $retries); do
        if curl -sf "$url" > /dev/null 2>&1; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

start_service() {
    PORT_COUNTER=$((PORT_COUNTER + 1))
    PORT=$((8010 + PORT_COUNTER))
    export PORT
    local LOG_FILE
    LOG_FILE=$(mktemp)
    "$PROJECT_DIR/target/debug/svc-notifier" > "$LOG_FILE" 2>&1 &
    SVC_PID=$!
    if ! wait_for_health "http://localhost:$PORT/readyz"; then
        echo "ERROR: service did not start within 15s"
        echo "--- service logs ---"
        cat "$LOG_FILE"
        echo "--- end logs ---"
        rm -f "$LOG_FILE"
        return 1
    fi
    rm -f "$LOG_FILE"
}

stop_service() {
    if [ -n "$SVC_PID" ] && kill -0 "$SVC_PID" 2>/dev/null; then
        kill "$SVC_PID" 2>/dev/null
        wait "$SVC_PID" 2>/dev/null || true
        SVC_PID=""
    fi
    # Wait for port to be released
    for i in $(seq 1 20); do
        if ! lsof -i ":$PORT" -sTCP:LISTEN > /dev/null 2>&1; then
            return 0
        fi
        sleep 0.3
    done
}

reset_db() {
    psql "$DATABASE_URL_OWNER" -q -c "DROP TABLE IF EXISTS notifications CASCADE; DROP TABLE IF EXISTS _sqlx_migrations CASCADE;" 2>/dev/null || true
}

# --- Build (must succeed or abort) ---
echo "Building svc-notifier..."
cd "$PROJECT_DIR" || exit 1
cargo build --quiet || { echo "FATAL: cargo build failed"; exit 1; }

# ============================================================
echo ""
echo "=== P0: Startup and infrastructure ==="
# ============================================================

# --- P0.1: Liveness endpoint ---
echo ""
echo "[P0.1] Service starts and /livez returns 200"
reset_db
if start_service; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$PORT/livez")
    BODY=$(curl -s "http://localhost:$PORT/livez")

    if [ "$HTTP_CODE" = "200" ] && [ "$BODY" = "alive" ]; then
        log_pass "/livez returns 200 with body 'alive'"
    else
        log_fail "/livez returned $HTTP_CODE: $BODY"
    fi
else
    log_fail "service failed to start"
fi

stop_service

# --- P0.2: Migrations apply on empty database ---
echo ""
echo "[P0.2] Migrations apply on empty database"
reset_db
if start_service; then
    TABLE_EXISTS=$(psql "$DATABASE_URL_OWNER" -t -c "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = 'notifications');" 2>/dev/null | tr -d ' ')

    if [ "$TABLE_EXISTS" = "t" ]; then
        log_pass "notifications table created by migrations"
    else
        log_fail "notifications table not found after startup"
    fi

    # Check RLS is enabled
    RLS_ENABLED=$(psql "$DATABASE_URL_OWNER" -t -c "SELECT relrowsecurity FROM pg_class WHERE relname = 'notifications';" 2>/dev/null | tr -d ' ')

    if [ "$RLS_ENABLED" = "t" ]; then
        log_pass "RLS enabled on notifications table"
    else
        log_fail "RLS not enabled on notifications table"
    fi
else
    log_fail "service failed to start (migrations not tested)"
fi

stop_service

# --- P0.3: Service works without NATS_URL ---
echo ""
echo "[P0.3] Service starts without NATS_URL (graceful degradation)"
reset_db
unset NATS_URL

if start_service; then
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:$PORT/readyz")

    if [ "$HTTP_CODE" = "200" ]; then
        log_pass "service runs without NATS_URL"
    else
        log_fail "service failed without NATS_URL (HTTP $HTTP_CODE)"
    fi
else
    log_fail "service failed to start without NATS_URL"
fi

stop_service
export NATS_URL="nats://localhost:4222"

# ============================================================
echo ""
echo "=== Results ==="
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
echo ""

if [ "$FAIL" -gt 0 ]; then
    exit 1
fi
