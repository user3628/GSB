$ErrorActionPreference = "Stop"

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Root

Write-Host "[1/3] go mod download"
go mod download
if ($LASTEXITCODE -ne 0) { throw "go mod download failed" }

Write-Host "[2/3] go test ./..."
go test ./...
if ($LASTEXITCODE -ne 0) { throw "go test failed" }

Write-Host "[3/3] reproducible anomaly experiment"
go run ./cmd/experiment -root $Root -out artifacts/experiment
if ($LASTEXITCODE -ne 0) { throw "experiment failed" }

Write-Host "summary: $Root\artifacts\experiment\summary.json"
