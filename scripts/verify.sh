#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export GOPROXY=off GOSUMDB=off
go test ./... -count=1
mkdir -p bin tmp
go build -o bin/recallab ./cmd/recallab
VERIFY_DIR="$(mktemp -d tmp/verify.XXXXXX)"
./bin/recallab generate --out "$VERIFY_DIR/data" --seed 99 --items 200 --queries 40 --vector-dim 16
./bin/recallab run --data "$VERIFY_DIR/data" --db "$VERIFY_DIR/process.db" --out "$VERIFY_DIR/process.json" --workers 3 --mode process
./bin/recallab run --data "$VERIFY_DIR/data" --db "$VERIFY_DIR/inline.db" --out "$VERIFY_DIR/inline.json" --workers 1 --mode inline
cmp "$VERIFY_DIR/process.json" "$VERIFY_DIR/inline.json"
sha256sum "$VERIFY_DIR/process.json"
echo "Verification passed: process and inline outputs are byte-identical."
