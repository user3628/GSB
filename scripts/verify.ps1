# Windows equivalent of scripts/verify.sh
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "== go build ./... =="
go build ./...
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "== go test ./... =="
go test ./...
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "== end-to-end verification (fixed seed) =="
go run ./cmd/verifyrun
exit $LASTEXITCODE
