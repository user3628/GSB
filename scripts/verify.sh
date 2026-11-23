#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
OUT="$ROOT/artifacts/verification.txt"
mkdir -p "$ROOT/artifacts"

{
  echo 'ReleaseGate verification'
  echo "date=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "workspace=$ROOT"
  echo "go=$(go version)"
  echo
  echo '$ go test -count=1 ./...'
  go test -count=1 ./...
  echo
  echo '$ go test -race -count=1 ./...'
  go test -race -count=1 ./...
  echo
  echo '$ go vet ./...'
  go vet ./...
  echo
  echo '$ GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...'
  GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
  echo 'windows/amd64 cross-build: OK'
  echo
  echo '$ bash examples/run-demo.sh'
  bash examples/run-demo.sh
  echo
  echo '$ bash examples/run-go-run-smoke.sh'
  bash examples/run-go-run-smoke.sh
  echo
  echo '== durable demo facts =='
  printf 'forced curl exit=%s\n' "$(cat artifacts/demo/forced-curl-exit.txt)"
  printf 'forced server exit=%s\n' "$(cat artifacts/demo/forced-server-exit.txt)"
  echo "gate1=$(grep -o '"outcome":"[^"]*"' artifacts/demo/gate1.json | head -1)"
  echo "gate2=$(grep -o '"code":"[^"]*"' artifacts/demo/gate2-failure.json | head -1)"
  echo "recovery=$(grep -o '"pending_intents_found":[0-9]*' artifacts/demo/recovery-report.json | head -1),$(grep -o '"intents_recovered":[0-9]*' artifacts/demo/recovery-report.json | head -1)"
  echo "recovered_release=$(grep -o '"state":"[^"]*"' artifacts/demo/recovered-release.json | head -1)"
  echo "final_release=$(grep -o '"state":"[^"]*"' artifacts/demo/rolled-back.json | head -1)"
  echo
  echo 'verification: OK'
} 2>&1 | tee "$OUT"
