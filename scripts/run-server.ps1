# 单独启动工作台（可选：先手工启动 cmd/testupstream 作为回环后端）
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$env:CGO_ENABLED = "0"
go run ./cmd/server -addr 127.0.0.1:18080 -db data/workbench.db
