#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/artifacts/verification"
BIN="$ROOT/artifacts/lease-arbiter-bin"
mkdir -p "$OUT"
cd "$ROOT"

echo "[1/8] gofmt"
UNFORMATTED="$(gofmt -l cmd internal)"
if [[ -n "$UNFORMATTED" ]]; then
  echo "unformatted files:" >&2
  echo "$UNFORMATTED" >&2
  exit 1
fi

echo "[2/8] go vet"
go vet ./...

echo "[3/8] unit and integration tests"
go test ./... -count=1

echo "[4/8] race detector"
go test -race ./... -count=1

echo "[5/8] CGO-disabled tests (pure Go SQLite)"
CGO_ENABLED=0 go test ./... -count=1

echo "[6/8] CGO-disabled build"
CGO_ENABLED=0 go build -o "$BIN" ./cmd/lease-arbiter

echo "[7/8] deterministic demo with real loopback heal dispatch"
rm -f "$OUT/demo.db" "$OUT/demo.db-wal" "$OUT/demo.db-shm"
"$BIN" demo --db "$OUT/demo.db" >"$OUT/demo-report.json"
sqlite3 -header -column "$OUT/demo.db" \
  "SELECT version, snapshot_version, length(payload) AS payload_bytes FROM committed_states ORDER BY version;
   SELECT kind, state, base_version, target_version FROM intents ORDER BY created_at_ms, id;" \
  >"$OUT/demo-sqlite-facts.txt"

echo "[8/8] SIGKILL mid-batch and restart recovery"
"$ROOT/scripts/kill-mid-batch.sh" >"$OUT/crash-run.log"

cat >"$OUT/SUMMARY.txt" <<EOF
verification completed at $(date -Iseconds)
gofmt: clean
go vet: clean
go test: passed
go test -race: passed
CGO_ENABLED=0 test/build: passed
demo report: artifacts/verification/demo-report.json
demo sqlite facts: artifacts/verification/demo-sqlite-facts.txt
crash recovery report: artifacts/verification/crash/recovery-report.json
crash sqlite facts: artifacts/verification/crash/sqlite-facts.txt
EOF

echo "verification complete: $OUT/SUMMARY.txt"

