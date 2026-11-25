#!/usr/bin/env bash
set -euo pipefail

go version
go test ./... -count=1
go vet ./...
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o /tmp/sampleengine-windows.exe .
go run . demo --dir .demo \
  --summary artifacts/demo-summary.json \
  --report artifacts/rebuild-report.json

echo "Expected summary_digest: f1580c2da67d48acaf675c6b444c315d394f80b1579a35bc7e19ee05c69ce9d7"
