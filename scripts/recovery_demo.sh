#!/usr/bin/env bash
# End-to-end recovery acceptance demo:
# 1. generate deterministic dataset
# 2. start the service as an independent process
# 3. run a full reconstruction (continuous baseline)
# 4. force-kill the process, restart it, and verify resume + byte equivalence
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$ROOT/tmp-recovery"
ADDR="127.0.0.1:8791"
rm -rf "$WORK"
mkdir -p "$WORK"
cd "$ROOT"
go build -o "$ROOT/bin/voxelrecon" ./cmd/voxelrecon
"$ROOT/bin/voxelrecon" -workdir "$WORK" -addr "$ADDR" &
PID=$!
trap 'kill -9 $PID 2>/dev/null || true' EXIT
sleep 1
curl -fsS -X POST "$ADDR/api/dataset/generate" -H 'Content-Type: application/json' \
  -d '{"seed":2024,"shards":3,"views_per":3,"width":16,"height":16}' >/dev/null
echo "== full baseline =="
curl -fsS -X POST "$ADDR/api/reconstruct" -H 'Content-Type: application/json' \
  -d '{"force_full":true}' | tee "$WORK/baseline.json" >/dev/null
echo "== force killing process $PID =="
kill -9 "$PID"
wait "$PID" 2>/dev/null || true
echo "== restarting =="
"$ROOT/bin/voxelrecon" -workdir "$WORK" -addr "$ADDR" &
PID=$!
sleep 1
echo "== resumed adaptive run with equality check =="
curl -fsS -X POST "$ADDR/api/reconstruct" -H 'Content-Type: application/json' \
  -d '{"force_full":false,"verify_equality":true}'
echo
echo "== checkpoints =="
curl -fsS "$ADDR/api/checkpoints"
echo
kill "$PID" 2>/dev/null || true
echo "RECOVERY DEMO OK"
