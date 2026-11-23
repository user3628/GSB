#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${EDITOR_TEST_PORT:-18181}"
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

json_get() {
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"
}

cd "${ROOT}"
go build -o "${BIN}" ./cmd/editor
"${BIN}" -addr "127.0.0.1:${PORT}" -db "${DB}" >"${TMP}/server.log" 2>&1 &
PID=$!
for _ in $(seq 1 100); do
  if curl -fsS "${BASE}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done

curl -fsS -X POST -H 'Content-Type: text/plain' --data-binary @samples/plan_current.jsonl \
  "${BASE}/v1/snapshots/import" >"${TMP}/import.json"
[[ "$(json_get "${TMP}/import.json" version)" == "200" ]]

curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @samples/patch_001_set_predicate.json \
  "${BASE}/v1/patches" >"${TMP}/patch1.json"
curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @samples/patch_002_reorder_join.json \
  "${BASE}/v1/patches" >"${TMP}/patch2.json"
curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @samples/patch_003_add_filter.json \
  "${BASE}/v1/patches" >"${TMP}/patch3.json"
curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @samples/patch_004_delete_filter.json \
  "${BASE}/v1/patches" >"${TMP}/patch4.json"
[[ "$(json_get "${TMP}/patch4.json" version)" == "204" ]]

curl -fsS "${BASE}/v1/snapshot" >"${TMP}/snapshot.json"
curl -fsS "${BASE}/v1/indexes/columns" >"${TMP}/columns.json"
curl -fsS "${BASE}/v1/indexes/predicates" >"${TMP}/predicates.json"
curl -fsS "${BASE}/v1/estimates" >"${TMP}/estimates.json"
curl -fsS "${BASE}/v1/degradations" >"${TMP}/degradations.json"
curl -fsS "${BASE}/v1/recovery" >"${TMP}/recovery.json"
[[ "$(json_get "${TMP}/snapshot.json" version)" == "204" ]]
[[ "$(json_get "${TMP}/recovery.json" consistent)" == "True" ]]
echo "SMOKE PASS: formats/patches/indexes/estimates/recovery at version 204"
