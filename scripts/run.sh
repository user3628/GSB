#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export GOPROXY=off GOSUMDB=off

ITEMS="${ITEMS:-800}"
QUERIES="${QUERIES:-120}"
WORKERS="${WORKERS:-4}"
CONCURRENCY="${CONCURRENCY:-4}"
STRATEGY="${STRATEGY:-weighted}"
DATA_DIR="${DATA_DIR:-data}"
MODE="${MODE:-process}"

mkdir -p bin
go build -o bin/recallab ./cmd/recallab
./bin/recallab generate --out "$DATA_DIR" --seed 20261125 --items "$ITEMS" --queries "$QUERIES" --vector-dim 24 --strategy "$STRATEGY"
./bin/recallab run --data "$DATA_DIR" --db "$DATA_DIR/experiment.db" --out "$DATA_DIR/results.json" \
  --workers "$WORKERS" --concurrency "$CONCURRENCY" --mode "$MODE"
printf 'Results: %s\nDatabase: %s\n' "$DATA_DIR/results.json" "$DATA_DIR/experiment.db"
