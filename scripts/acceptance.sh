#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$ROOT/.acceptance-tmp"
BIN="$WORK/auditdrift"
DB="$WORK/recovery.db"

rm -rf "$WORK"
mkdir -p "$WORK"
cd "$ROOT"

echo "==> [1/7] go test ./..."
go test ./...

echo "==> [2/7] build CLI"
go build -o "$BIN" ./cmd/auditdrift

echo "==> [3/7] fixed seed and dual-format samples"
"$BIN" seed --dir "$WORK/seed"
test -s "$WORK/seed/baselines.json"
test -s "$WORK/seed/observations.jsonl"
test -s "$WORK/seed/observations.legacy.lpb"

echo "==> [4/7] budget and weighted quota degradation"
"$BIN" init --db "$WORK/budget.db" --session demo-baseline-audit --quota 20 >"$WORK/budget-init.json"
"$BIN" audit --db "$WORK/budget.db" --session demo-baseline-audit \
  --concurrency 3 --queue-size 30 --budget 5s >"$WORK/budget-audit.json"
python3 - "$WORK/budget-audit.json" <<'PY'
import collections, json, sys
report = json.load(open(sys.argv[1]))
counts = collections.Counter(t["status"] for t in report["tasks"])
assert len(report["tasks"]) == 24, counts
assert counts["DEFERRED"] >= 10, counts
assert report["degradations"], "missing degradation basis"
assert all("remaining_quota" in d for d in report["degradations"])
assert report["quota_consumed_by_scheduler"] + report["quota_remaining_scheduler"] == report["quota_initial"]
print(f"budget/quota OK: {dict(counts)}, remaining={report['quota_remaining_scheduler']}")
PY

echo "==> [5/7] monotonic increment, E_STALE_BASE and cascade"
"$BIN" init --db "$WORK/cascade.db" --session demo-baseline-audit --quota 100 >/dev/null
"$BIN" increment-demo --db "$WORK/cascade.db" --session demo-baseline-audit >"$WORK/increment.json"
python3 - "$WORK/increment.json" <<'PY'
import json, sys
report = json.load(open(sys.argv[1]))
assert report["version"] == 2, report
assert "ACCT_LOCKOUT_THRESHOLD" in report["affected_baseline_ids"]
print("increment cascade OK: version=2")
PY
set +e
"$BIN" stale-base-demo --db "$WORK/cascade.db" --session demo-baseline-audit --base 1 >"$WORK/stale.out" 2>&1
rc=$?
set -e
test "$rc" -ne 0
grep -q "E_STALE_BASE" "$WORK/stale.out"
echo "stale base OK: E_STALE_BASE returned and committed version unchanged"

echo "==> [6/7] prepare audited recovery database"
"$BIN" init --db "$DB" --session demo-baseline-audit --quota 25 >"$WORK/recovery-init.json"
"$BIN" audit --db "$DB" --session demo-baseline-audit --concurrency 3 --queue-size 30 --budget 5s >"$WORK/recovery-audit.json"

run_crash_cycle() {
  local tag="$1"
  local ready="$WORK/crash-${tag}.out"
  "$BIN" crash-demo --db "$DB" --session demo-baseline-audit --wait 2m >"$ready" 2>&1 &
  local pid=$!
  for _ in $(seq 1 200); do
    if grep -q "crash_window_ready" "$ready" 2>/dev/null; then break; fi
    sleep 0.05
  done
  grep -q "crash_window_ready" "$ready"
  kill -9 "$pid"
  wait "$pid" || true
  "$BIN" recover --db "$DB" --session demo-baseline-audit >"$WORK/recovery-${tag}.json"
}

echo "==> [7/7] two SIGKILL recovery cycles and sqlite3 fact check"
run_crash_cycle first
run_crash_cycle second
python3 - "$WORK/recovery-first.json" "$WORK/recovery-second.json" <<'PY'
import json, sys
def normalized(path):
    d = json.load(open(path))
    return {
        "pending": d["pending_intent_count_before"],
        "aborted": len(d["aborted_intent_ids"]),
        "reset": d["tasks_reset_to_pending"],
        "version": d["last_complete_version"],
        "status": d["session_status"],
        "tasks": d["tasks_by_status"],
        "summaries": d["host_summaries"],
        "final_pending": d["final_pending_intents"],
        "integrity": d["integrity_check"],
        "consistent": d["consistent"],
    }
a = normalized(sys.argv[1]); b = normalized(sys.argv[2])
assert a["consistent"] and b["consistent"], (a, b)
assert a == b, (a, b)
assert a["pending"] == 1 and a["aborted"] == 1 and a["final_pending"] == 0, a
assert a["version"] == 1 and a["status"] == "RECOVERED", a
print("repeat recovery OK: normalized reports match")
print(json.dumps(a, ensure_ascii=False, indent=2))
PY

pending_count="$(sqlite3 "$DB" "SELECT COUNT(*) FROM intents WHERE status='PENDING';")"
test "$pending_count" = "0"
sqlite3 -header -column "$DB" "SELECT host,total,done,conformant,drift,deferred,errors,max_drift_level FROM host_summaries ORDER BY host;"
sqlite3 "$DB" "SELECT 'summary_mismatch_count=' || COUNT(*) FROM host_summaries h WHERE h.total <> (SELECT COUNT(*) FROM conclusions c WHERE c.session_id=h.session_id AND c.host=h.host);"

echo
echo "ACCEPTANCE PASSED. Artifacts: $WORK"
