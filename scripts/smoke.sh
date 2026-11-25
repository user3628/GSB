#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== go version =="
go version
echo "== fixed generator =="
go run ./cmd/platform generate --out artifacts/seed.json
echo "== sqlite/workdir/loopback smoke =="
go run ./cmd/platform smoke --db artifacts/smoke.db
