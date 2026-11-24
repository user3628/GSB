#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

BIN="${BIN:-bin/authlab}"
if [[ ! -x "$BIN" ]]; then
  go build -trimpath -o "$BIN" ./cmd/authlab
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

"$BIN" experiment --out "$TMP/run-1" "$@" >/dev/null
"$BIN" experiment --out "$TMP/run-2" "$@" >/dev/null

cmp "$TMP/run-1/report.json" "$TMP/run-2/report.json"
diff -ru "$TMP/run-1/states" "$TMP/run-2/states"
cmp "$TMP/run-1/report.sha256" "$TMP/run-2/report.sha256"

sha256sum "$TMP/run-1/report.json"
echo "deterministic report and state exports are byte-identical"
