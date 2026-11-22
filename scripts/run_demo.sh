#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PYTHON="$ROOT/.venv/bin/python"
if [[ -z "${PORT:-}" ]]; then
  PORT="$("$PYTHON" -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
fi
DB="${DEMO_DB:-$ROOT/data/demo.db}"
LOG="$ROOT/artifacts/demo-server.log"
STOP="$ROOT/data/.demo-stop"
PID_FILE="$ROOT/data/.demo-server.pid"
if [[ ! -x "$PYTHON" ]]; then
  echo "missing .venv; create it with Python 3.11 and install requirements first" >&2
  exit 1
fi

rm -f "$DB" "$DB-wal" "$DB-shm" "$STOP" "$PID_FILE" "$LOG"
rm -f "$ROOT"/artifacts/demo-snapshot-v*.json "$ROOT/artifacts/demo-recovery-report.json" "$ROOT/artifacts/demo-summary.json"
mkdir -p "$ROOT/data" "$ROOT/artifacts"

export STREAM_AGG_DB="$DB"
export ENABLE_DEMO_CRASH=1
export PYTHONPATH="$ROOT"

cleanup() {
  touch "$STOP"
  if [[ -f "$PID_FILE" ]]; then
    kill "$(cat "$PID_FILE")" 2>/dev/null || true
  fi
  kill "$SUPERVISOR_PID" 2>/dev/null || true
  wait "$SUPERVISOR_PID" 2>/dev/null || true
  rm -f "$STOP" "$PID_FILE"
}
trap cleanup EXIT INT TERM

(
  while [[ ! -f "$STOP" ]]; do
    "$PYTHON" -m uvicorn app.main:app --host 127.0.0.1 --port "$PORT" >>"$LOG" 2>&1 &
    SERVER_PID=$!
    echo "$SERVER_PID" > "$PID_FILE"
    set +e
    wait "$SERVER_PID"
    STATUS=$?
    set -e
    if [[ -f "$STOP" ]]; then
      break
    fi
    echo "uvicorn exited with status $STATUS; supervisor restarting" >>"$LOG"
    sleep 0.35
  done
) &
SUPERVISOR_PID=$!

echo "Starting demo service on http://127.0.0.1:$PORT (db=$DB)"
echo "Server log: $LOG"

"$PYTHON" scripts/demo.py --base-url "http://127.0.0.1:$PORT" --db "$DB"

echo
echo "Running pytest after the demo..."
STREAM_AGG_DB="$ROOT/data/test-import.db" "$PYTHON" -m pytest -q

echo
echo "Demo and tests completed successfully."
