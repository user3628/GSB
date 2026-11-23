$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

go run ./cmd/demo -report artifacts/demo-report.json @args
