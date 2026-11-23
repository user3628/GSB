#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${EDITOR_TEST_PORT:-18180}"
BASE="http://127.0.0.1:${PORT}"
TMP="$(mktemp -d)"
DB="${TMP}/editor.db"
BIN="${TMP}/editor"
PID=""

cleanup() {
  if [[ -n "${PID}" ]] && kill -0 "${PID}" 2>/dev/null; then
    kill "${PID}" 2>/dev/null || true
    wait "${PID}" 2>/dev/null || true
  fi
  rm -rf "${TMP}"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  echo "--- server log ---" >&2
  cat "${TMP}/server.log" >&2 || true
  exit 1
}

json_get() {
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}

start_editor() {
  "${BIN}" -addr "127.0.0.1:${PORT}" -db "${DB}" >>"${TMP}/server.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 100); do
    if curl -fsS "${BASE}/healthz" >"${TMP}/health.json" 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  fail "editor did not become healthy"
}

stop_editor() {
  if [[ -n "${PID}" ]] && kill -0 "${PID}" 2>/dev/null; then
    kill "${PID}" 2>/dev/null || true
    wait "${PID}" 2>/dev/null || true
  fi
  PID=""
}

crash_request() {
  local method="$1"
  local path="$2"
  local file="$3"
  local headers="$4"
  set +e
  # shellcheck disable=SC2086
  curl -sS -X "${method}" ${headers} --data-binary "@${file}" "${BASE}${path}" >"${TMP}/crash-response.txt" 2>"${TMP}/curl.err"
  local curl_status=$?
  wait "${PID}" 2>/dev/null
  local process_status=$?
  set -e
  PID=""
  if [[ ${process_status} -ne 137 ]]; then
    fail "expected process exit 137, got ${process_status} (curl ${curl_status})"
  fi
}

arm_crash() {
  local stage="$1"
  curl -fsS -X POST -H 'Content-Type: application/json' \
    --data "{\"stage\":\"${stage}\",\"mode\":\"crash\"}" \
    "${BASE}/v1/admin/crash-point" >"${TMP}/arm.json"
}

cd "${ROOT}"
go build -o "${BIN}" ./cmd/editor
start_editor

curl -fsS -X POST -H 'Content-Type: text/plain' -H 'X-Request-ID: rehearsal-base' \
  --data-binary @samples/plan_current.jsonl "${BASE}/v1/snapshots/import" >"${TMP}/import-base.json"
[[ "$(json_get "${TMP}/import-base.json" version)" == "200" ]] || fail "base import version"
echo "PASS base import version=200"

# Termination point 1: durable patch intent exists, snapshot/derived rows do not.
arm_crash patch_after_intent
crash_request POST /v1/patches samples/patch_001_set_predicate.json "-H 'Content-Type: application/json'"
echo "PASS process terminated at patch_after_intent (exit 137)"

start_editor
curl -fsS "${BASE}/v1/recovery" >"${TMP}/recovery-patch.json"
[[ "$(json_get "${TMP}/recovery-patch.json" pending_found)" == "1" ]] || fail "patch recovery pending_found"
[[ "$(json_get "${TMP}/recovery-patch.json" current_version)" == "200" ]] || fail "patch recovery version"
[[ "$(json_get "${TMP}/recovery-patch.json" consistent)" == "True" ]] || fail "patch recovery consistency"
curl -fsS "${BASE}/v1/snapshot" >"${TMP}/snapshot-after-patch-crash.json"
[[ "$(json_get "${TMP}/snapshot-after-patch-crash.json" version)" == "200" ]] || fail "patch crash leaked a version"
curl -fsS "${BASE}/v1/tree" >"${TMP}/tree-after-patch-crash.json"
grep -q 'orders.amount > 100' "${TMP}/tree-after-patch-crash.json" || fail "old predicate is not readable"
echo "PASS restart restored complete version 200 and aborted the patch intent"

curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @samples/patch_001_set_predicate.json \
  "${BASE}/v1/patches" >"${TMP}/patch-after-recovery.json"
[[ "$(json_get "${TMP}/patch-after-recovery.json" version)" == "201" ]] || fail "post-recovery patch"
echo "PASS patch can be retried after recovery at version=201"

# Termination point 2: durable import intent exists for snapshot sequence 300.
arm_crash import_after_intent
crash_request POST /v1/snapshots/import samples/plan_next.jsonl "-H 'Content-Type: text/plain'"
echo "PASS process terminated at import_after_intent (exit 137)"

start_editor
curl -fsS "${BASE}/v1/recovery" >"${TMP}/recovery-import.json"
[[ "$(json_get "${TMP}/recovery-import.json" pending_found)" == "1" ]] || fail "import recovery pending_found"
[[ "$(json_get "${TMP}/recovery-import.json" current_version)" == "201" ]] || fail "import recovery version"
[[ "$(json_get "${TMP}/recovery-import.json" consistent)" == "True" ]] || fail "import recovery consistency"
curl -fsS "${BASE}/v1/snapshot" >"${TMP}/snapshot-after-import-crash.json"
[[ "$(json_get "${TMP}/snapshot-after-import-crash.json" version)" == "201" ]] || fail "import crash leaked a version"
echo "PASS restart restored complete version 201 and aborted the import intent"

curl -fsS -X POST -H 'Content-Type: text/plain' -H 'X-Request-ID: rehearsal-next' \
  --data-binary @samples/plan_next.jsonl "${BASE}/v1/snapshots/import" >"${TMP}/import-next.json"
[[ "$(json_get "${TMP}/import-next.json" version)" == "300" ]] || fail "post-recovery import"
curl -fsS "${BASE}/v1/snapshot" >"${TMP}/snapshot-final.json"
[[ "$(json_get "${TMP}/snapshot-final.json" version)" == "300" ]] || fail "final snapshot version"
echo "PASS import can be retried after recovery at version=300"

stop_editor
echo "ALL CRASH-RECOVERY CHECKS PASSED"
