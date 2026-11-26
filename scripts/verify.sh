#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
export GOFLAGS="${GOFLAGS:--mod=vendor}"

echo "== gofmt =="
unformatted="$(gofmt -l cmd internal)"
if [[ -n "$unformatted" ]]; then
  echo "$unformatted"
  exit 1
fi

echo "== go vet =="
go vet ./...

echo "== go test =="
go test -count=1 ./...

echo "== go test -race =="
go test -race -count=1 ./...

echo "== build =="
mkdir -p bin
go build -o bin/logagg ./cmd/logagg

echo "ALL VERIFICATION STEPS PASSED"
