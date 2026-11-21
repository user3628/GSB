#!/usr/bin/env bash
# Deterministic end-to-end demo: shards -> reference -> diagnostics -> injections.
set -euo pipefail
cd "$(dirname "$0")/.."
PY=../.venv/bin/python
PORT="${PORT:-8792}"

echo "[1/6] generate 3 deterministic shards (B=2,H=4,L=32,D=16)"
$PY -m posbias.cli generate --shards 3 --batch 2 --heads 4 \
    --seq-len 32 --head-dim 16

echo "[2/6] build golden reference metrics from local shards"
$PY -m posbias.cli reference

echo "[3/6] run four-stage diagnostic pipeline"
$PY -m posbias.cli run

echo "[4/6] legacy v1 manifest import / export round-trip"
$PY -m posbias.cli legacy-import >/dev/null
$PY -m posbias.cli legacy-export >/dev/null

echo "[5/6] fault injection cases"
for kind in seq_too_long invalid_scale zero_bins nan_shard zero_base_freq zero_quant_scale; do
  $PY -m posbias.cli inject "$kind" \
    | $PY -c "import sys,json;d=json.load(sys.stdin);print(d['kind'],'->',d['error']['code'])"
done

echo "[6/6] test suite"
$PY -m unittest tests.test_workbench -v 2>&1 | tail -5
