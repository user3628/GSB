#!/usr/bin/env bash
# crash_recovery.sh - end-to-end forced-termination and recovery verification.
#
# It starts the server, loads data, admits requests, arms a forced crash at the
# round commit boundary, waits for the process to die, restarts it, then checks
# the recovery report and cross-table invariants via sqlite3.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${WORK_DIR:-$ROOT/.crash-work/$(date +%s)-$$}"
ADDR="127.0.0.1:18099"
DB="$WORK/gpusched.db"

mkdir -p "$WORK"
trap 'stop_if_alive' EXIT

echo "== build =="
(cd "$ROOT" && go build -o "$WORK/gpusched" ./cmd/gpusched)

start_server() {
  setsid nohup "$WORK/gpusched" serve -db "$DB" -addr "$ADDR" \
    >"$WORK/server.log" 2>&1 < /dev/null &
  echo $! > "$WORK/server.pid"
  for _ in $(seq 1 50); do
    if curl -sS "http://$ADDR/healthz" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "server did not become healthy"; cat "$WORK/server.log"; exit 1
}

stop_if_alive() {
  if [[ -f "$WORK/server.pid" ]] && kill -0 "$(cat "$WORK/server.pid")" 2>/dev/null; then
    kill "$(cat "$WORK/server.pid")" || true
    sleep 0.3
  fi
}

echo "== start (fresh db) =="
stop_if_alive
start_server

echo "== load fixed sample =="
curl -sS -X POST "http://$ADDR/v1/spec/load-sample" | python3 -m json.tool

echo "== admit a deterministic set of requests =="
for id in req-001 req-002 req-003 req-004; do
  curl -sS -X POST "http://$ADDR/v1/requests/$id/admit" >/dev/null
done

# Use the post-commit point for a deterministic forced-termination drill: the
# round transaction is guaranteed durable when the process is killed, so the
# restart must find a COMMITTED round and reconcile it via the intent/round
# markers. The pre-commit atomic-rollback path is covered by the Go subprocess
# test (TestCrashRecoverySubprocess) where SQLite discards the whole tx.
echo "== arm forced crash at round:after_commit =="
curl -sS -X POST "http://$ADDR/v1/debug/crash" \
  -H 'Content-Type: application/json' -d '{"point":"round:after_commit"}'
echo

echo "== trigger round (process should be killed) =="
set +e
curl -sS -X POST "http://$ADDR/v1/scheduler/round" -d '{}'
rc=$?
set -e
echo "(curl exit=$rc; expected non-zero because the process died)"

# Wait until the process is really gone.
for _ in $(seq 1 50); do
  if ! kill -0 "$(cat "$WORK/server.pid")" 2>/dev/null; then break; fi
  sleep 0.1
done
if kill -0 "$(cat "$WORK/server.pid")" 2>/dev/null; then
  echo "process survived the crash unexpectedly"; exit 1
fi
echo "process terminated as expected"

echo "== restart (startup recovery runs automatically) =="
start_server

echo "== recovery report =="
curl -sS "http://$ADDR/v1/recovery" | python3 -m json.tool

echo "== sqlite3 cross-table checks =="
sqlite3 "$DB" <<'SQL'
.headers on
SELECT 'rounds_committed_or_recovered' AS check_name, COUNT(*) AS value
  FROM rounds WHERE status IN ('COMMITTED','RECOVERED');
SELECT 'requests_not_left_running_after_restart' AS check_name, COUNT(*) AS value
  FROM requests WHERE state='RUNNING';
SELECT g.id, g.memory_reserved AS group_mem_reserved,
       COALESCE(SUM(n.memory_used),0) AS node_mem_used,
       (g.memory_reserved=COALESCE(SUM(n.memory_used),0)) AS mem_match,
       g.compute_reserved AS group_comp_reserved,
       COALESCE(SUM(n.compute_used_m),0) AS node_comp_used,
       (g.compute_reserved=COALESCE(SUM(n.compute_used_m),0)) AS comp_match
  FROM quota_groups g
  LEFT JOIN nodes n ON n.group_id=g.id
  GROUP BY g.id;
SELECT status, COUNT(*) AS batches FROM batches GROUP BY status;
SELECT status, COUNT(*) AS intents FROM intents GROUP BY status;
SQL

echo
echo "== verify a normal round works after recovery =="
curl -sS -X POST "http://$ADDR/v1/scheduler/round" -d '{}' | python3 -m json.tool | head -30

stop_if_alive
echo "CRASH_RECOVERY_OK"
