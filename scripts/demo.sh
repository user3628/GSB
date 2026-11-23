#!/usr/bin/env bash
# 网关权重调度与并行探测工作台 - 本地回环演示（Linux/macOS，Windows 请用 demo.ps1）
set -euo pipefail
cd "$(dirname "$0")/.."
export CGO_ENABLED=0
go version
go test ./...
go run ./cmd/demo
echo "demo finished; SQLite and intent log are under ./data"
