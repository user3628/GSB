# Windows PowerShell demo: no CGO, no Docker, only local loopback endpoints.
$ErrorActionPreference = "Stop"

$RootDir = Split-Path -Parent $PSScriptRoot
Set-Location $RootDir

$env:CGO_ENABLED = "0"
$env:GOSUMDB = "off"
# If your module cache is pre-populated on an offline machine:
# $env:GOPROXY = "off"

Write-Host "==> Running Go tests"
go test ./...
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

New-Item -ItemType Directory -Force -Path "bin", "artifacts/results" | Out-Null

Write-Host "==> Building Windows platform binary"
go build -o "bin/platform.exe" ./cmd/platform
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> Running HTTP/process-kill/recovery end-to-end demo"
go run ./cmd/e2e `
  -platform-bin (Join-Path $RootDir "bin/platform.exe") `
  -workdir (Join-Path $RootDir "artifacts/e2e-runtime") `
  -out (Join-Path $RootDir "artifacts/results/e2e-result.json")
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> Demo report: artifacts/results/e2e-result.json"
