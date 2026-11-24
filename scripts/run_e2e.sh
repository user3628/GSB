#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SAMPLE=${SAMPLE:-"$ROOT/samples/pool_sample.json"}
ADDR=${ADDR:-127.0.0.1:19090}
ARTIFACTS="$ROOT/artifacts"
TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/sqlitepool-e2e.XXXXXX")
DATA_DIR="$TMP_DIR/data"
BIN_DIR="$TMP_DIR/bin"
RESULT_FILE="$ARTIFACTS/e2e-verification.json"
LOG_FILE="$ARTIFACTS/e2e-run.log"

mkdir -p "$DATA_DIR" "$BIN_DIR" "$ARTIFACTS"

echo "[1/4] go version: $(go version)"
echo "[2/4] building poolserver and e2edemo"
go build -trimpath -o "$BIN_DIR/poolserver" "$ROOT/cmd/poolserver"
go build -trimpath -o "$BIN_DIR/e2edemo" "$ROOT/cmd/e2edemo"

echo "[3/4] running deterministic loopback scenario at $ADDR"
set +e
"$BIN_DIR/e2edemo" \
  -sample "$SAMPLE" \
  -data-dir "$DATA_DIR" \
  -addr "$ADDR" \
  -server-bin "$BIN_DIR/poolserver" \
  -result-file "$RESULT_FILE" 2>&1 | tee "$LOG_FILE"
STATUS=${PIPESTATUS[0]}
set -e

if [[ -f "$DATA_DIR/recovery-latest.json" ]]; then
  cp "$DATA_DIR/recovery-latest.json" "$ARTIFACTS/recovery-latest.json"
fi
if [[ -f "$DATA_DIR/server-2.log" ]]; then
  cp "$DATA_DIR/server-2.log" "$ARTIFACTS/server-restart.log"
fi

echo "[4/4] verification result: $RESULT_FILE"
echo "       execution log:       $LOG_FILE"
echo "       isolated data dir:   $DATA_DIR"
exit "$STATUS"
