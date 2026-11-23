#!/usr/bin/env sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
OUT=${OUT:-"$ROOT/data"}

cd "$ROOT"
exec go run ./cmd/samplegen -out "$OUT" "$@"
