#!/usr/bin/env bash
set -euo pipefail

echo "== Go environment =="
go version
echo "GOOS=$(go env GOOS)"

echo "== Build/test/verify =="
go mod tidy
go build ./...
go test ./...
go run ./cmd/verify

echo "== Windows cross-build =="
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...

echo "PASS"
