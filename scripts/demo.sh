#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export GOFLAGS=-mod=vendor
export GOPROXY=off
export CGO_ENABLED=0

RUN_ID="$(date +%Y%m%d-%H%M%S)"
OUT="$ROOT/artifacts/demo-$RUN_ID"
mkdir -p "$OUT" "$OUT/bin" samples

log() { printf '[demo] %s\n' "$*"; }
cleanup() {
  if [[ -n "${SERVER_PID:-}" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -TERM "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

wait_http() {
  local url="$1"
  local attempts=100
  while (( attempts > 0 )); do
    if curl -fsS --max-time 1 "$url/api/health" >/dev/null 2>&1; then
      return 0
    fi
    attempts=$((attempts - 1))
    sleep 0.1
  done
  return 1
}

json_py() {
  python3 - "$@"
}

log "building offline with vendored SQLite driver"
go version > "$OUT/tool-versions.txt"
sqlite3 --version >> "$OUT/tool-versions.txt"
go build -mod=vendor -trimpath -o "$OUT/bin/windowd" ./cmd/windowd
go build -mod=vendor -trimpath -o "$OUT/bin/windowctl" ./cmd/windowctl
"$OUT/bin/windowctl" generate -seed 242 -out "$ROOT/samples/deterministic.json" | tee "$OUT/sample-generate.txt"
cp "$ROOT/samples/deterministic.json" "$OUT/sample.json"

PORT="$(free_port)"
BASE="http://127.0.0.1:$PORT"
log "starting full-round server at $BASE"
"$OUT/bin/windowd" -db "$OUT/full.db" -addr "127.0.0.1:$PORT" -max-global-concurrency 16 \
  > "$OUT/full-server.log" 2>&1 &
SERVER_PID=$!
echo "$SERVER_PID" > "$OUT/full-server.pid"
wait_http "$BASE" || { cat "$OUT/full-server.log"; exit 1; }

curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @"$OUT/sample.json" \
  "$BASE/api/snapshots" > "$OUT/load.json"
curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @- \
  "$BASE/api/recalc" > "$OUT/full-recalc.json" <<'JSON'
{
  "snapshot_id": "det-sample-seed-242",
  "budgets": {
    "global_concurrency": 4,
    "tenant_concurrency": 2,
    "time_budget_ms": 10000,
    "queue_capacity": 512,
    "task_delay_ms": 1
  }
}
JSON
FULL_ROUND="$(python3 - "$OUT/full-recalc.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))["round"]["round_id"])
PY
)"
curl -fsS "$BASE/api/schedule?round_id=$FULL_ROUND" > "$OUT/full-schedule.json"
curl -fsS "$BASE/api/aggregates?snapshot_id=det-sample-seed-242&tenant_id=tenant-b" > "$OUT/full-aggregates-tenant-b.json"
python3 - "$OUT/full-recalc.json" "$OUT/full-aggregates-tenant-b.json" <<'PY'
import json, sys
round_result = json.load(open(sys.argv[1]))
aggregate = json.load(open(sys.argv[2]))
r = round_result["round"]
assert r["status"] == "COMPLETED", r
assert r["shed_count"] == 0, r
assert r["committed_count"] > 0, r
assert r["failed_count"] == 0, r
assert r["consumed_budget_ms"] <= r["time_budget_ms"], r
assert r["max_global_running"] <= r["global_concurrency"], r
assert r["max_tenant_running"] <= r["tenant_concurrency"], r
assert aggregate["version"] > 0 and aggregate["windows"], aggregate
assert all(w["status"] == "COMMITTED" and w["aggregate"] for w in aggregate["windows"])
print(f'full round: committed={r["committed_count"]} shed={r["shed_count"]} accounted={r["consumed_budget_ms"]}ms tenant_version={aggregate["version"]}')
PY

log "running deliberately budget-constrained round"
curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @- \
  "$BASE/api/recalc" > "$OUT/shed-recalc.json" <<'JSON'
{
  "snapshot_id": "det-sample-seed-242",
  "force": true,
  "budgets": {
    "global_concurrency": 2,
    "tenant_concurrency": 1,
    "time_budget_ms": 5,
    "queue_capacity": 512,
    "task_delay_ms": 0
  }
}
JSON
SHED_ROUND="$(python3 - "$OUT/shed-recalc.json" <<'PY'
import json, sys
print(json.load(open(sys.argv[1]))["round"]["round_id"])
PY
)"
curl -fsS "$BASE/api/schedule?round_id=$SHED_ROUND" > "$OUT/shed-schedule.json"
python3 - "$OUT/shed-recalc.json" "$OUT/shed-schedule.json" <<'PY'
import json, sys
result = json.load(open(sys.argv[1]))
schedule = json.load(open(sys.argv[2]))
r = result["round"]
assert r["shed_count"] > 0, r
assert r["consumed_budget_ms"] <= r["time_budget_ms"] == 5, r
assert r["remaining_budget_ms"] >= 0, r
assert len(schedule["shed_decisions"]) == r["shed_count"], schedule
assert all(d["reason_code"] == "BUDGET_EXHAUSTED" for d in schedule["shed_decisions"])
shed_tasks = [t for t in result["tasks"] if t["status"] == "SHED"]
assert shed_tasks and all(t["consumed_ms"] == 0 for t in shed_tasks)
print(f'shed round: committed={r["committed_count"]} shed={r["shed_count"]} accounted={r["consumed_budget_ms"]}ms remaining={r["remaining_budget_ms"]}ms')
PY

sqlite3 "$OUT/full.db" > "$OUT/sqlite-spotcheck.txt" <<'SQL'
.headers on
.mode column
SELECT status, COUNT(*) AS task_count, SUM(consumed_ms) AS consumed_ms FROM recalc_tasks GROUP BY status ORDER BY status;
SELECT status, COUNT(*) AS intent_count FROM intents GROUP BY status ORDER BY status;
SELECT snapshot_id, tenant_id, version, total_count, total_sum_scaled, committed_window_count, group_count FROM tenant_intermediate ORDER BY tenant_id;
SELECT COUNT(*) AS committed_aggregates FROM window_aggregates;
SELECT COUNT(*) AS group_index_rows FROM group_index;
SQL
cat "$OUT/sqlite-spotcheck.txt"

log "stopping full-round server"
kill -TERM "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""

log "starting crash-recovery scenario"
PORT="$(free_port)"
BASE="http://127.0.0.1:$PORT"
"$OUT/bin/windowd" -db "$OUT/crash.db" -addr "127.0.0.1:$PORT" -max-global-concurrency 16 \
  > "$OUT/crash-server.log" 2>&1 &
SERVER_PID=$!
echo "$SERVER_PID" > "$OUT/crash-server.pid"
wait_http "$BASE" || { cat "$OUT/crash-server.log"; exit 1; }
curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @"$OUT/sample.json" \
  "$BASE/api/snapshots" > "$OUT/crash-load.json"

curl -sS --max-time 30 -X POST -H 'Content-Type: application/json' --data-binary @- \
  "$BASE/api/recalc" > "$OUT/crash-recalc-request.json" 2> "$OUT/crash-curl-error.txt" <<'JSON' &
{
  "snapshot_id": "det-sample-seed-242",
  "budgets": {
    "global_concurrency": 2,
    "tenant_concurrency": 1,
    "time_budget_ms": 10000,
    "queue_capacity": 512,
    "task_delay_ms": 250
  }
}
JSON
CURL_PID=$!

PRE_KILL_OK=0
for _ in $(seq 1 120); do
  if curl -fsS "$BASE/api/intents?status=PENDING" > "$OUT/pre-kill-intents.json" 2>/dev/null; then
    COUNT="$(python3 - "$OUT/pre-kill-intents.json" <<'PY'
import json, sys
print(len(json.load(open(sys.argv[1]))["intents"]))
PY
)"
    if (( COUNT >= 1 )); then
      PRE_KILL_OK=1
      break
    fi
  fi
  sleep 0.1
done
if (( PRE_KILL_OK == 0 )); then
  echo "no pending start intent observed before kill" >&2
  cat "$OUT/crash-server.log" >&2
  exit 1
fi
curl -fsS "$BASE/api/rounds?limit=1" > "$OUT/pre-kill-rounds.json"
curl -fsS "$BASE/api/intents" > "$OUT/pre-kill-all-intents.json"
PRE_KILL_PENDING="$(python3 - "$OUT/pre-kill-intents.json" <<'PY'
import json, sys
print(len(json.load(open(sys.argv[1]))["intents"]))
PY
)"
log "force killing server with $PRE_KILL_PENDING pending intent(s)"
kill -9 "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
wait "$CURL_PID" 2>/dev/null || true
SERVER_PID=""

sleep 0.2
log "restarting against the same SQLite file for startup recovery"
PORT="$(free_port)"
BASE="http://127.0.0.1:$PORT"
"$OUT/bin/windowd" -db "$OUT/crash.db" -addr "127.0.0.1:$PORT" -max-global-concurrency 16 \
  > "$OUT/crash-restart-server.log" 2>&1 &
SERVER_PID=$!
echo "$SERVER_PID" > "$OUT/crash-restart-server.pid"
wait_http "$BASE" || { cat "$OUT/crash-restart-server.log"; exit 1; }
curl -fsS "$BASE/api/recovery" > "$OUT/recovery-report.json"
curl -fsS "$BASE/api/intents" > "$OUT/post-recovery-intents.json"
curl -fsS "$BASE/api/rounds?limit=1" > "$OUT/post-recovery-rounds.json"
python3 - "$OUT/pre-kill-intents.json" "$OUT/recovery-report.json" "$OUT/post-recovery-intents.json" "$OUT/post-recovery-rounds.json" <<'PY'
import json, sys
pre = json.load(open(sys.argv[1]))
report = json.load(open(sys.argv[2]))
intents = json.load(open(sys.argv[3]))["intents"]
rounds = json.load(open(sys.argv[4]))["rounds"]
assert report["based_on_durable_facts"] is True, report
assert report["pending_intent_count"] >= 1, report
assert report["aborted_task_ids"], report
assert report["reset_window_ids"], report
assert report["interrupted_round_ids"], report
assert not [i for i in intents if i["status"] == "PENDING"], intents
assert any(i["status"] == "ROLLED_BACK" for i in intents), intents
assert rounds and rounds[0]["status"] == "INTERRUPTED", rounds
print(f'recovery: pre_kill_pending={len(pre["intents"])} aborted_tasks={len(report["aborted_task_ids"])} reset_windows={len(report["reset_window_ids"])} interrupted_rounds={len(report["interrupted_round_ids"])}')
PY

log "re-enqueuing recovered PENDING windows in a fresh round"
curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @- \
  "$BASE/api/recalc" > "$OUT/recovery-recalc.json" <<'JSON'
{
  "snapshot_id": "det-sample-seed-242",
  "budgets": {
    "global_concurrency": 4,
    "tenant_concurrency": 2,
    "time_budget_ms": 10000,
    "queue_capacity": 512,
    "task_delay_ms": 0
  }
}
JSON
python3 - "$OUT/recovery-recalc.json" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))["round"]
assert r["shed_count"] == 0, r
assert r["failed_count"] == 0, r
print(f'post-recovery round: considered={r["considered_count"]} committed={r["committed_count"]} accounted={r["consumed_budget_ms"]}ms')
PY

sqlite3 "$OUT/crash.db" > "$OUT/sqlite-recovery-check.txt" <<'SQL'
.headers on
.mode column
SELECT status, COUNT(*) AS intent_count FROM intents GROUP BY status ORDER BY status;
SELECT status, COUNT(*) AS task_count FROM recalc_tasks GROUP BY status ORDER BY status;
SELECT COUNT(*) AS pending_intents FROM intents WHERE status = 'PENDING';
SELECT COUNT(*) AS committed_aggregates FROM window_aggregates;
SELECT report_id, pending_intent_count, based_on_durable_facts FROM recovery_reports ORDER BY created_at_ms;
SQL
cat "$OUT/sqlite-recovery-check.txt"

kill -TERM "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""

python3 - "$OUT" "$RUN_ID" > "$OUT/SUMMARY.md" <<'PY'
import json, pathlib, sys
out = pathlib.Path(sys.argv[1])
run_id = sys.argv[2]
full = json.load(open(out / "full-recalc.json"))["round"]
shed = json.load(open(out / "shed-recalc.json"))["round"]
recovery = json.load(open(out / "recovery-report.json"))
recovery_round = json.load(open(out / "recovery-recalc.json"))["round"]
print("# Demo result")
print()
print(f"- Run: {run_id}")
print(f"- Full round: {full['committed_count']} committed, {full['shed_count']} shed, {full['consumed_budget_ms']} ms accounted.")
print(f"- Budget round: {shed['shed_count']} shed, {shed['consumed_budget_ms']} ms consumed of {shed['time_budget_ms']} ms, {shed['remaining_budget_ms']} ms remaining.")
print(f"- Recovery: {recovery['pending_intent_count']} pending intent(s), {len(recovery['aborted_task_ids'])} aborted task(s), {len(recovery['reset_window_ids'])} reset window(s), {len(recovery['interrupted_round_ids'])} interrupted round(s).")
print(f"- Post-recovery round: {recovery_round['committed_count']} committed, {recovery_round['failed_count']} failed, {recovery_round['shed_count']} shed.")
print("- Evidence: `recovery-report.json`, `sqlite-spotcheck.txt`, `sqlite-recovery-check.txt`.")
print("- No exit-time report was promised; the recovery report was generated from durable SQLite rows on restart.")
PY

log "demo complete: $OUT"
cat "$OUT/SUMMARY.md"
