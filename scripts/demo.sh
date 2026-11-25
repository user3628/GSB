#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
go test ./...
go run ./cmd/demo -seed 20261125 -events 64
