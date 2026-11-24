#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT_WAS_SET="${PORT:+1}"
PORT="${PORT:-18082}"
port_available() {
  python3 - "$1" <<'PY'
import socket, sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
except OSError:
    sys.exit(1)
finally:
    s.close()
PY
}
if ! port_available "${PORT}"; then
  if [[ -n "${PORT_WAS_SET}" ]]; then
    echo "port ${PORT} is not available; choose another PORT" >&2
    exit 1
  fi
  PORT="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
  echo "default port 18082 busy; using ${PORT}"
fi
ADDR="127.0.0.1:${PORT}"
BASE_URL="http://${ADDR}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/replicabudget-demo.XXXXXX")"
DB="${WORK}/policies.db"
BIN="${WORK}/policyd"
SERVER_PID=""
SERVER_MODE=""

cleanup() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -TERM "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  if [[ "${KEEP_TMP:-0}" != "1" ]]; then
    rm -rf "${WORK}"
  else
    echo "demo workspace kept at ${WORK}"
  fi
}
trap cleanup EXIT

cd "${ROOT}"
go build -o "${BIN}" ./cmd/policyd
go build -o "${WORK}/samplegen" ./cmd/samplegen
"${WORK}/samplegen" -jsonl samples/new.jsonl -out "${WORK}/regenerated.bin" >/dev/null
cmp "${WORK}/regenerated.bin" samples/new.bin

wait_ready() {
  for _ in $(seq 1 100); do
    if curl -fsS "${BASE_URL}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.1
  done
  echo "server did not become ready" >&2
  return 1
}

start_server() {
  local mode="$1"
  shift
  env "$@" "${BIN}" -addr "${ADDR}" -db "${DB}" >"${WORK}/server-${mode}.log" 2>&1 &
  SERVER_PID=$!
  SERVER_MODE="${mode}"
  wait_ready
}

stop_server() {
  if [[ -n "${SERVER_PID}" ]] && kill -0 "${SERVER_PID}" 2>/dev/null; then
    kill -TERM "${SERVER_PID}" 2>/dev/null || true
    wait "${SERVER_PID}" 2>/dev/null || true
  fi
  SERVER_PID=""
}

post_file() {
  local name="$1"
  local url="$2"
  local file="$3"
  curl -sS -o "${WORK}/${name}.json" -w '%{http_code}' -X POST --data-binary @"${file}" "${url}"
}

assert_code() {
  local expected="$1"
  local actual="$2"
  local name="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    echo "${name}: expected HTTP ${expected}, got ${actual}" >&2
    cat "${WORK}/${name}.json" >&2 || true
    exit 1
  fi
}

start_server normal
echo "server: ${BASE_URL} db=${DB}"

code=$(curl -sS -o "${WORK}/compat.json" -w '%{http_code}' "${BASE_URL}/v1/compatibility")
assert_code 200 "${code}" compat

code=$(post_file quotas "${BASE_URL}/v1/quota-groups?base_version=0" samples/quotas.json)
assert_code 200 "${code}" quotas
code=$(post_file migrate "${BASE_URL}/v1/migrate?base_version=1" samples/legacy.json)
assert_code 200 "${code}" migrate
code=$(post_file jsonl "${BASE_URL}/v1/import?base_version=2" samples/new.jsonl)
assert_code 200 "${code}" jsonl
code=$(post_file binary "${BASE_URL}/v1/import?base_version=3" samples/new.bin)
assert_code 200 "${code}" binary
code=$(post_file incremental "${BASE_URL}/v1/incremental" samples/incremental_weight.json)
assert_code 200 "${code}" incremental

code=$(post_file ambiguous "${BASE_URL}/v1/migrate?base_version=5" samples/legacy_ambiguous.json)
assert_code 409 "${code}" ambiguous
code=$(post_file unsupported "${BASE_URL}/v1/import?base_version=5" samples/new_v2_unknown.jsonl)
assert_code 415 "${code}" unsupported

body_a='{"base_version":5,"patches":[{"service_id":"svc-a","weight_permille":1100}]}'
body_b='{"base_version":5,"patches":[{"service_id":"svc-a","weight_permille":1100}]}'
curl -sS -o "${WORK}/concurrent-a.json" -w '%{http_code}' -X POST --data-binary "${body_a}" "${BASE_URL}/v1/incremental" >"${WORK}/concurrent-a.code" &
pid_a=$!
curl -sS -o "${WORK}/concurrent-b.json" -w '%{http_code}' -X POST --data-binary "${body_b}" "${BASE_URL}/v1/incremental" >"${WORK}/concurrent-b.code" &
pid_b=$!
wait "${pid_a}"
wait "${pid_b}"
code_a=$(cat "${WORK}/concurrent-a.code")
code_b=$(cat "${WORK}/concurrent-b.code")

python3 - "${WORK}" "${code_a}" "${code_b}" <<'PY'
import json, sys
work, ca, cb = sys.argv[1], sys.argv[2], sys.argv[3]
codes = sorted([ca, cb])
assert codes == ["200", "409"], codes
stale = 'a' if ca == '409' else 'b'
with open(f"{work}/concurrent-{stale}.json") as f:
    err = json.load(f)["error"]
assert err["code"] == "E_STALE_BASE", err
with open(f"{work}/migrate.json") as f:
    migrate = json.load(f)
assert migrate["migration"]["decision"] == "accept"
with open(f"{work}/jsonl.json") as f:
    jsonl_state = json.load(f)["state"]
with open(f"{work}/binary.json") as f:
    binary_state = json.load(f)["state"]
assert jsonl_state["weighted_index"]["index_micros"] == binary_state["weighted_index"]["index_micros"] == 4833333
assert [(p["strategy"]["service_id"], p["desired_replicas"]) for p in jsonl_state["policies"]] == [
    (p["strategy"]["service_id"], p["desired_replicas"]) for p in binary_state["policies"]]
with open(f"{work}/incremental.json") as f:
    incremental = json.load(f)
assert incremental["affected_services"] == ["svc-b"]
assert incremental["state"]["weighted_index"]["index_micros"] == 5222222
assert incremental["state"]["threshold"]["exceeded"] is True
with open(f"{work}/ambiguous.json") as f:
    ambiguous = json.load(f)["error"]
assert ambiguous["code"] == "E_MIGRATION_AMBIGUOUS"
eq = ambiguous["details"]["numeric_mismatches"][0]["equivalence"]
assert (eq["fixed_replicas"], eq["expected_replicas"], eq["difference"], eq["within_tolerance"]) == (6, 2, 4, False)
with open(f"{work}/unsupported.json") as f:
    unsupported = json.load(f)["error"]
assert unsupported["code"] == "E_FORMAT_UNSUPPORTED"
print(f"dual-format index: {jsonl_state['weighted_index']['index_micros']} (jsonl == binary)")
print("migration: accepted; degraded weight=1000; ambiguous numeric mismatch=4 replicas")
print(f"cascade: svc-b weight changed; index {incremental['state']['weighted_index']['index_micros']} > threshold")
print(f"concurrency: one success, one E_STALE_BASE (codes {ca}/{cb})")
PY

echo "--- crash recovery"
curl -sS "${BASE_URL}/v1/state" >"${WORK}/pre-crash-state.json"
pre_version=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["version"])' "${WORK}/pre-crash-state.json")
stop_server
start_server failpoint POLICY_FAILPOINT=mid_rebuild
cat >"${WORK}/crash-incremental.json" <<'JSON'
{"base_version":6,"patches":[{"service_id":"svc-b","weight_permille":3100}]}
JSON
set +e
curl -sS --max-time 5 -X POST --data-binary @"${WORK}/crash-incremental.json" "${BASE_URL}/v1/incremental" >"${WORK}/crash-response.txt" 2>/dev/null
crash_curl=$?
wait "${SERVER_PID}"
crash_exit=$?
SERVER_PID=""
set -e
if [[ "${crash_exit}" != "86" ]]; then
  echo "expected failpoint exit 86, got ${crash_exit}" >&2
  exit 1
fi
start_server recovered
curl -sS "${BASE_URL}/v1/state" >"${WORK}/post-crash-state.json"
curl -sS "${BASE_URL}/v1/recovery-report" >"${WORK}/recovery.json"

python3 - "${WORK}" "${pre_version}" "${crash_curl}" <<'PY'
import json, sys
work, pre_version, crash_curl = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
post = json.load(open(f"{work}/post-crash-state.json"))
report = json.load(open(f"{work}/recovery.json"))
assert post["version"] == pre_version, (post["version"], pre_version)
weights = {p["strategy"]["service_id"]: p["strategy"]["weight_permille"] for p in post["policies"]}
assert weights["svc-b"] != 3100, weights
assert post["weighted_index"]["index_micros"] == 5195652, post["weighted_index"]
assert report["current_version"] == pre_version
assert report["last_complete_version"] == pre_version
assert len(report["recovered_intents"]) == 1, report
assert report["recovered_intents"][0]["previous_status"] == "pending"
assert report["current_version"] == report["last_complete_version"]
print(f"crash: curl_exit={crash_curl}, process_exit=86, committed version stayed {pre_version}")
print(f"recovery: action={report['action']}, intent={report['recovered_intents'][0]['kind']}, last_complete={report['last_complete_version']}")
PY

echo "--- final state summary"
python3 - "${WORK}/post-crash-state.json" <<'PY'
import json, sys
state = json.load(open(sys.argv[1]))
print(json.dumps({
  "version": state["version"],
  "policies": [{"service_id": p["strategy"]["service_id"], "desired_replicas": p["desired_replicas"]} for p in state["policies"]],
  "weighted_index_micros": state["weighted_index"]["index_micros"],
  "references": len(state["references"]),
}, ensure_ascii=False, indent=2))
PY
