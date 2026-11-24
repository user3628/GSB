#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

OUT="${OUT:-artifacts/run}"
BIN="${BIN:-bin/authlab}"

go build -trimpath -o "$BIN" ./cmd/authlab
"$BIN" experiment --out "$OUT" "$@"
