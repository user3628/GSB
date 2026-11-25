#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARTIFACTS="$ROOT/artifacts/verification/crash"
DB="$ARTIFACTS/crash.db"
SENTINEL="$ARTIFACTS/dispatch.ready"
BIN="$(mktemp "$ROOT/.lease-arbiter-crash.XXXXXX")"
mkdir -p "$ARTIFACTS"
rm -f "$DB" "$DB-wal" "$DB-shm" "$SENTINEL"
rm -f "$ARTIFACTS"/*.json "$ARTIFACTS"/*.log "$ARTIFACTS"/*.txt
trap 'rm -f "$BIN"' EXIT

cd "$ROOT"
go build -o "$BIN" ./cmd/lease-arbiter

"$BIN" crash-mid-batch \
  --db "$DB" \
  --sentinel "$SENTINEL" \
  >"$ARTIFACTS/before-kill.json" \
  2>"$ARTIFACTS/before-kill.log" &
PID=$!

READY=0
for _ in $(seq 1 200); do
  if [[ -f "$SENTINEL" ]]; then
    READY=1
    break
  fi
  if ! kill -0 "$PID" 2>/dev/null; then
    break
  fi
  sleep 0.05
done

if [[ "$READY" -ne 1 ]]; then
  echo "heal dispatch never became observable; process log follows" >&2
  cat "$ARTIFACTS/before-kill.log" >&2 || true
  kill -9 "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  exit 1
fi

kill -9 "$PID"
wait "$PID" 2>/dev/null || true

# Re-open the same database in a new process. Recover must derive its report
# solely from the PREPARED intent and last complete committed row.
"$BIN" recover --db "$DB" >"$ARTIFACTS/recovery-report.json"

sqlite3 -header -column "$DB" \
  "SELECT version, snapshot_version, length(payload) AS payload_bytes FROM committed_states ORDER BY version;
   SELECT id, kind, state, base_version, target_version FROM intents ORDER BY created_at_ms, id;
   SELECT COUNT(*) AS recovery_reports FROM recovery_reports;" \
  >"$ARTIFACTS/sqlite-facts.txt"

echo "forced termination verification written to $ARTIFACTS"
cat "$ARTIFACTS/sqlite-facts.txt"

