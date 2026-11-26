#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
mkdir -p bin
CGO_ENABLED=1 go build -trimpath -o bin/dnscache ./cmd/dnscache
CGO_ENABLED=1 go build -trimpath -o bin/authserver ./cmd/authserver
CGO_ENABLED=1 go build -trimpath -o bin/dnsprobe ./cmd/dnsprobe
CGO_ENABLED=1 go build -trimpath -o bin/zonehash ./cmd/zonehash
printf 'built: %s/bin/{dnscache,authserver,dnsprobe,zonehash}\n' "$ROOT"
