#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

DATA="$(mktemp -d /tmp/netpol-verify.XXXXXX)"
PORT="${VERIFY_PORT:-18087}"
BASE="http://127.0.0.1:${PORT}"
BIN="$DATA/netpol-compiler"
PID=""

echo "DATA_DIR=$DATA"
echo "GO_VERSION=$(go version)"
echo "SQLITE_VERSION=$(pkg-config --modversion sqlite3)"

go build -o "$BIN" ./cmd/netpol-compiler

stop_server() {
  if [[ -n "$PID" ]]; then
    kill -TERM "$PID" 2>/dev/null || true
    for _ in $(seq 1 50); do
      kill -0 "$PID" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
    PID=""
  fi
}
trap stop_server EXIT

wait_health() {
  for _ in $(seq 1 100); do
    if curl -sf "$BASE/healthz" >/dev/null 2>&1; then
      return 0
    fi
    if [[ -n "$PID" ]] && ! kill -0 "$PID" 2>/dev/null; then
      echo "server exited; log:" >&2
      cat "$SERVER_LOG" >&2 || true
      return 1
    fi
    sleep 0.1
  done
  echo "server did not become healthy; log:" >&2
  cat "$SERVER_LOG" >&2 || true
  return 1
}

start_server() {
  SERVER_LOG="$1"
  shift
  "$BIN" --addr "127.0.0.1:${PORT}" --data-dir "$DATA/db" "$@" >"$SERVER_LOG" 2>&1 &
  PID=$!
  wait_health
}

post_json() {
  local path="$1" file="$2" output="$3"
  curl -sS -X POST "$BASE$path" -H 'Content-Type: application/json' --data @"$file" >"$output"
}

extract_data() {
  local source="$1" target="$2"
  python3 - "$source" "$target" <<'PY'
import json, sys
source, target = sys.argv[1], sys.argv[2]
with open(source) as handle:
    envelope = json.load(handle)
with open(target, "w") as handle:
    json.dump(envelope["data"], handle)
PY
}

echo
echo '== 1. fixed seed sample: compile index, ports, conflicts, consistency =='
start_server "$DATA/server-1.log" --seed-sample
curl -sS "$BASE/v1/views/demo?observationVersion=1" >"$DATA/view1.json"
python3 - "$DATA/view1.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))["data"]
v = d["view"]
print(f"usedVersion={d['usedVersion']} namespace={v['namespace']} rules={v['ruleCount']}")
print(f"ports raw={v['portView']['totalRawCount']} merged={v['portView']['totalMergedCount']} mergeRule={v['portView']['mergeRule'][:70]}...")
for p in v["portView"]["protocols"]:
    print(f"  {p['protocol']}: raw={p['rawCount']} merged={p['mergedCount']} events={len(p['mergeEvents'])}")
print(f"selectorEntries={len(v['selectorIndex']['entries'])} containments={len(v['selectorIndex']['containments'])} conflictPairs={len(v['adjudication']['conflictPairs'])}")
print(f"consistency scoreQ={v['consistency']['scoreQ']} decimal={v['consistency']['scoreDecimal']} cumulativeError={v['consistency']['cumulativeRoundingError']}")
assert d["usedVersion"] == 1
assert v["ruleCount"] == 9
assert v["portView"]["totalRawCount"] == 9
assert v["portView"]["totalMergedCount"] == 5
assert v["selectorIndex"]["containments"]
assert v["adjudication"]["conflictPairs"]
PY

echo
echo '== 2. per-port allow/deny adjudication =='
cat >"$DATA/eval-request.json" <<'JSON'
{
  "namespace": "demo",
  "sourceLabels": {"role": "edge", "env": "prod"},
  "destinationLabels": {"app": "web", "tier": "frontend"},
  "protocol": "TCP",
  "ports": [8083, 8087, 8092]
}
JSON
curl -sS -X POST "$BASE/v1/adjudications?observationVersion=1" -H 'Content-Type: application/json' --data @"$DATA/eval-request.json" >"$DATA/eval.json"
python3 - "$DATA/eval.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))["data"]["decision"]
for p in d["portDecisions"]:
    print(f"port={p['port']} action={p['action']} winner={p.get('winnerRuleId')} hit={p['matchedRuleIds']}")
actions = {p["port"]: p["action"] for p in d["portDecisions"]}
assert actions == {8083: "allow", 8087: "allow", 8092: "deny"}
assert len(d["portDecisions"][1]["matchedRuleIds"]) == 2
print(f"decisionDigest={d['decisionDigest']}")
PY

echo
echo '== 3. invalid rule reports E_RULE_INVALID and first field =='
cat >"$DATA/invalid.json" <<'JSON'
{
  "version": 900,
  "generatedAt": "2026-11-26T00:00:00Z",
  "digest": "sha256:unused-because-validation-runs-first",
  "rules": [{
    "namespace": "demo",
    "id": "bad-action",
    "action": "maybe",
    "source": {},
    "destination": {},
    "protocol": "TCP",
    "priority": 10
  }]
}
JSON
post_json /v1/snapshots "$DATA/invalid.json" "$DATA/invalid-response.json"
cat "$DATA/invalid-response.json"
python3 - "$DATA/invalid-response.json" <<'PY'
import json, sys
e = json.load(open(sys.argv[1]))["error"]
assert e["code"] == "E_RULE_INVALID" and e["field"] == "rules[0].action"
print("\ninvalid-field assertion=OK")
PY

echo
echo '== 4. stale full snapshot is rejected without state change =='
curl -sS "$BASE/v1/sample/snapshot?version=1" >"$DATA/snapshot-v1-envelope.json"
extract_data "$DATA/snapshot-v1-envelope.json" "$DATA/snapshot-v1.json"
post_json /v1/snapshots "$DATA/snapshot-v1.json" "$DATA/stale-snapshot.json"
cat "$DATA/stale-snapshot.json"
python3 - "$DATA/stale-snapshot.json" <<'PY'
import json, sys
e = json.load(open(sys.argv[1]))["error"]
assert e["code"] == "E_STALE_SNAPSHOT"
assert e["context"]["currentVersion"] == 1
print("\nstale-snapshot assertion=OK")
PY

echo
echo '== 5. local deterministic increment commits version 2 =='
curl -sS "$BASE/v1/sample/increment?baseVersion=1&requestId=verify-increment-1" >"$DATA/increment-envelope.json"
extract_data "$DATA/increment-envelope.json" "$DATA/increment.json"
post_json /v1/increments "$DATA/increment.json" "$DATA/increment-response.json"
python3 - "$DATA/increment-response.json" <<'PY'
import json, sys
body = json.load(open(sys.argv[1]))
assert body["ok"], body
d = body["data"]
print(f"kind={d['kind']} version={d['version']} base={d['baseVersion']} namespaces={d['affectedNamespaces']}")
assert d["version"] == 2 and d["affectedNamespaces"] == ["demo"]
PY
curl -sS "$BASE/v1/views/demo?observationVersion=2" >"$DATA/view2.json"
python3 - "$DATA/view2.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))["data"]
print(f"usedVersion={d['usedVersion']} rules={d['view']['ruleCount']}")
assert d["usedVersion"] == 2 and d["view"]["ruleCount"] == 10
PY

echo
echo '== 6. consistency fixed-point explanation and threshold comparison =='
curl -sS "$BASE/v1/consistency?observationVersion=2&threshold=0.75&relativeTolerancePpm=50000" >"$DATA/consistency.json"
python3 - "$DATA/consistency.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))["data"]
for item in d["report"]["namespaces"]:
    print(f"{item['namespace']}: scoreQ={item['scoreQ']} score={item['scoreDecimal']} conflictRatioQ={item['conflictRatioQ']} mergeRateQ={item['mergeRateQ']} finalError={item['cumulativeRoundingError']}")
print("threshold states:", {k: v["state"] for k, v in d["thresholdEvaluation"]["decisions"].items()})
assert d["report"]["namespaces"]
assert d["thresholdEvaluation"]["decisions"]
PY

stop_server

echo
echo '== 7. observed version waits on complete v2 but rejects incomplete v3 rebuild =='
start_server "$DATA/server-delay.log" --rebuild-delay=2s
curl -sS "$BASE/v1/sample/snapshot?version=3" >"$DATA/snapshot-v3-envelope.json"
extract_data "$DATA/snapshot-v3-envelope.json" "$DATA/snapshot-v3.json"
curl -sS -X POST "$BASE/v1/snapshots" -H 'Content-Type: application/json' --data @"$DATA/snapshot-v3.json" >"$DATA/snapshot-v3-response.json" &
POST_PID=$!
STALE_OK=0
for _ in $(seq 1 40); do
  curl -sS "$BASE/v1/views/demo?observationVersion=3" >"$DATA/stale-read.json" || true
  if grep -q E_STALE_READ "$DATA/stale-read.json"; then STALE_OK=1; break; fi
  sleep 0.1
done
wait "$POST_PID"
cat "$DATA/stale-read.json"
python3 - "$DATA/stale-read.json" <<'PY'
import json, sys
e = json.load(open(sys.argv[1]))["error"]
assert e["code"] == "E_STALE_READ"
assert e["context"]["viewVersion"] == 2
assert e["context"]["observedVersion"] == 3
assert e["context"]["targetVersion"] == 3
print("\nstale-read-during-rebuild assertion=OK")
PY
test "$STALE_OK" -eq 1
curl -sS "$BASE/v1/views/demo?observationVersion=3" >"$DATA/view3.json"
python3 - "$DATA/view3.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))["data"]
assert d["usedVersion"] == 3
print(f"post-commit view usedVersion={d['usedVersion']} digest={d['view']['compiledDigest']}")
PY
stop_server

echo
echo '== 8. SIGKILL mid compilation after durable intent, restart recovery, then idempotent retry =='
start_server "$DATA/server-crash.log" --crash-point=mid_compile
curl -sS "$BASE/v1/sample/snapshot?version=4" >"$DATA/snapshot-v4-envelope.json"
extract_data "$DATA/snapshot-v4-envelope.json" "$DATA/snapshot-v4.json"
set +e
curl -sS --max-time 5 -X POST "$BASE/v1/snapshots" \
  -H 'Content-Type: application/json' -H 'X-Request-ID: verify-crash-v4' \
  --data @"$DATA/snapshot-v4.json" >"$DATA/crash-post.out" 2>"$DATA/crash-curl.err"
CURL_RC=$?
set -e
echo "curl_exit_after_sigkill=$CURL_RC (empty reply/reset is expected)"
wait "$PID" 2>/dev/null || true
PID=""
sleep 0.2

start_server "$DATA/server-recovery.log"
curl -sS "$BASE/v1/recovery" >"$DATA/recovery.json"
python3 - "$DATA/recovery.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))["data"]
print(f"status={r['status']} lastCompleteVersion={r['lastCompleteVersion']} unfinished={len(r['unfinishedIntents'])} aborted={r['abortedIntentIds']}")
for intent in r["unfinishedIntents"]:
    print(f"intent={intent['intentId']} kind={intent['kind']} target={intent['targetVersion']} snapshotPresent={intent['snapshotPresent']} readyViews={intent['readyViewRows']}")
assert r["status"] == "recovered_unfinished_intents"
assert r["lastCompleteVersion"] == 3
assert len(r["abortedIntentIds"]) == 1
assert r["unfinishedIntents"][0]["snapshotPresent"] is False
PY

curl -sS -X POST "$BASE/v1/snapshots" \
  -H 'Content-Type: application/json' -H 'X-Request-ID: verify-crash-v4' \
  --data @"$DATA/snapshot-v4.json" >"$DATA/retry-v4-response.json"
python3 - "$DATA/retry-v4-response.json" <<'PY'
import json, sys
body = json.load(open(sys.argv[1]))
assert body["ok"], body
assert body["data"]["version"] == 4
print(f"idempotent retry committed version={body['data']['version']} requestId=verify-crash-v4")
PY
curl -sS "$BASE/v1/views/demo?observationVersion=4" >"$DATA/view4.json"
python3 - "$DATA/view4.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))["data"]
assert d["usedVersion"] == 4
print(f"recovered view usedVersion={d['usedVersion']} digest={d['view']['compiledDigest']}")
PY
stop_server

echo
echo '== 9. Go unit/integration and race-focused service tests =='
go test ./... -count=1
go test -race ./... -count=1
go test ./internal/service -run 'TestConcurrentIncrementsSameBaseOnlyOneSucceeds|TestSameRequestIDConcurrentExecutesOnce|TestStaleReadWhileCascadePending|TestRecoveryAbortsPreparedIntent' -count=1 -v

echo
echo "VERIFICATION_OK data_dir=$DATA"
