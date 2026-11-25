#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "== go test =="
go test ./...

echo "== build =="
mkdir -p bin
go build -o bin/csrouter ./cmd/csrouter

VROOT="$(mktemp -d /tmp/csrouter-verify.XXXXXX)"
CROOT="$(mktemp -d /tmp/csrouter-continuous.XXXXXX)"
RROOT="$(mktemp -d /tmp/csrouter-resume.XXXXXX)"
echo "== sample root: $VROOT =="

echo "== generate/import =="
./bin/csrouter generate --root "$VROOT" >/tmp/csrouter-generate.json
./bin/csrouter validate --root "$VROOT"

echo "== run tiers =="
./bin/csrouter run --root "$VROOT" --tier small
./bin/csrouter run --root "$VROOT" --tier medium

echo "== deterministic rerun =="
h1="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["report_fingerprint"])' "$VROOT/reports/report-small.json")"
./bin/csrouter run --root "$VROOT" --tier small >/tmp/csrouter-rerun.json
h2="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["report_fingerprint"])' "$VROOT/reports/report-small.json")"
test "$h1" = "$h2"
echo "small rerun fingerprint: $h2"

echo "== stop/resume byte comparison =="
./bin/csrouter generate --root "$CROOT" >/dev/null
./bin/csrouter generate --root "$RROOT" >/dev/null
./bin/csrouter run --root "$CROOT" --tier small >/dev/null
./bin/csrouter run --root "$RROOT" --tier small --stop-after 41
./bin/csrouter resume --root "$RROOT" --tier small >/dev/null
cmp "$CROOT/reports/report-small.json" "$RROOT/reports/report-small.json"
echo "continuous and resumed reports are byte-identical"

echo "== verification passed =="
echo "reports: $VROOT/reports"
