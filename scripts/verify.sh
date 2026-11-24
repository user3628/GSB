#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "== go version =="
go version

echo "== module dependencies =="
go mod download

echo "== gofmt check =="
unformatted="$(gofmt -l .)"
if [[ -n "$unformatted" ]]; then
  echo "These files are not gofmt-formatted:" >&2
  echo "$unformatted" >&2
  exit 1
fi

echo "== go vet =="
go vet ./...

echo "== go build =="
go build ./...

echo "== go test =="
go test ./... -count=1

echo "== deterministic local demo =="
go run ./cmd/abeval demo --out-dir artifacts/demo

echo "== local library/CLI example =="
go run ./examples/local

echo "Verification completed successfully."
