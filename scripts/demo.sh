#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d "${TMPDIR:-/tmp}/blocksnap-e2e.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

cd "$ROOT"
go build -o "$TMP/blocksnapd" ./cmd/blocksnapd
go build -o "$TMP/blocksnap-demo" ./cmd/demo

# The demo starts the component HTTP listener, drives fixed-seed API calls,
# terminates blocksnapd with exit code 86 during a merge, restarts it on the
# same SQLite database, and verifies recovery + byte-identical metrics.
"$TMP/blocksnap-demo" -server-bin "$TMP/blocksnapd" -workdir "$TMP/data"
