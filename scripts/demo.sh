#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
PORT="${PORT:-18082}"
BASE_URL="http://127.0.0.1:${PORT}"
DB_PATH="${TMP}/profile.db"
STATE_FILE="${TMP}/demo-state.json"
SERVER_BIN="${TMP}/profile-server"
DEMO_BIN="${TMP}/profile-demo"
SERVER_PID=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -TERM "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT

start_server() {
  local crash_mode="${1:-0}"
  local log_file="${2:-${TMP}/server.log}"
  if [[ "${crash_mode}" == "1" ]]; then
    PROFILE_CRASH_STAGE=during_reservoir_replace "${SERVER_BIN}" -db "${DB_PATH}" -addr "127.0.0.1:${PORT}" >"${log_file}" 2>&1 &
  else
    "${SERVER_BIN}" -db "${DB_PATH}" -addr "127.0.0.1:${PORT}" >"${log_file}" 2>&1 &
  fi
  SERVER_PID=$!
  for _ in $(seq 1 100); do
    if curl -fsS "${BASE_URL}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      cat "${log_file}"
      return 1
    fi
    sleep 0.1
  done
  cat "${log_file}"
  return 1
}

stop_server() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -TERM "${SERVER_PID}"
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  SERVER_PID=""
}

cd "${ROOT}"
echo "== build =="
go build -o "${SERVER_BIN}" ./cmd/server
go build -o "${DEMO_BIN}" ./cmd/demo

echo "== phase 1: exact -> approximate, differences, cascade rebuild =="
start_server 0 "${TMP}/server-phase1.log"
"${DEMO_BIN}" -base-url "${BASE_URL}" -phase full -state-file "${STATE_FILE}"
stop_server

echo "== phase 2: forced termination during reservoir replacement =="
start_server 1 "${TMP}/server-crash.log"
"${DEMO_BIN}" -base-url "${BASE_URL}" -phase crash -state-file "${STATE_FILE}"
set +e
wait "${SERVER_PID}"
CRASH_STATUS=$?
set -e
SERVER_PID=""
echo "crash server exit status=${CRASH_STATUS}"
if [[ "${CRASH_STATUS}" -eq 0 ]]; then
  echo "expected a forced nonzero termination" >&2
  exit 1
fi

echo "== phase 3: restart, recovery report, idempotent retry =="
start_server 0 "${TMP}/server-recovery.log"
"${DEMO_BIN}" -base-url "${BASE_URL}" -phase verify -state-file "${STATE_FILE}"
curl -fsS "${BASE_URL}/v1/recovery/report"
echo
curl -fsS "${BASE_URL}/v1/versions/2/snapshot?service=checkout&limit=5" >"${TMP}/version-2-snapshot.json"
echo "version-2 snapshot written to ${TMP}/version-2-snapshot.json"
stop_server

echo "artifacts: ${TMP}"
echo "DEMO_SCRIPT_OK"
