#!/usr/bin/env bash
set -euo pipefail

go version
GOPROXY=off go test ./...
go vet ./...
go run ./cmd/speakerdiar run --workdir ./demo-workspace --verify-incremental
go run ./cmd/speakerdiar verify --workdir ./demo-workspace

echo "All verification steps passed."
