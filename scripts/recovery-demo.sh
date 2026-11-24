#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d /tmp/ledger-recovery-demo.XXXXXX)"
cd "$ROOT"
echo "workdir=$WORKDIR"

go build -o "$WORKDIR/ledger-workbench" ./cmd/ledger-workbench
DB="$WORKDIR/recovery.db"

"$WORKDIR/ledger-workbench" --db "$DB" --addr 127.0.0.1:0 --port-file "$WORKDIR/url1" &
PID1=$!
cleanup() { kill "${PID2:-0}" 2>/dev/null || true; }
trap cleanup EXIT

for _ in $(seq 1 100); do [[ -s "$WORKDIR/url1" ]] && break; sleep 0.05; done
URL1="$(cat "$WORKDIR/url1")"
for _ in $(seq 1 100); do curl -fsS "$URL1/healthz" >/dev/null 2>&1 && break; sleep 0.05; done

echo "== prime durable intent/staged rows, then SIGKILL self =="
curl -sS -H 'Content-Type: application/json' \
  -d '{"commit_id":"demo-crash","staged_entry_count":3,"kill_delay_ms":200}' \
  "$URL1/v1/admin/crash" || true
echo
wait "$PID1" 2>/dev/null || true

"$WORKDIR/ledger-workbench" --db "$DB" --addr 127.0.0.1:0 --port-file "$WORKDIR/url2" &
PID2=$!
for _ in $(seq 1 100); do [[ -s "$WORKDIR/url2" ]] && break; sleep 0.05; done
URL2="$(cat "$WORKDIR/url2")"
for _ in $(seq 1 100); do curl -fsS "$URL2/healthz" >/dev/null 2>&1 && break; sleep 0.05; done

echo "== recovery report from SQLite only =="
curl -sS "$URL2/v1/recovery"
echo

if command -v sqlite3 >/dev/null 2>&1; then
  echo "== sqlite facts =="
  sqlite3 -header -column "$DB" \
    "SELECT intent_id,commit_id,kind,status FROM intents;
     SELECT state,COUNT(*) AS count FROM ledger_entries GROUP BY state;
     SELECT migration_id,status FROM migrations;"
fi
echo "recovery demo complete; db=$DB"
