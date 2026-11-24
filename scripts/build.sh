#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
export GOPROXY=off GOSUMDB=off
go test ./...
mkdir -p bin
go build -trimpath -ldflags '-s -w' -o bin/driftctl ./cmd/driftctl
printf 'built bin/driftctl\n'
