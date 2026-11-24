$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

go test ./... -count=1
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

go test -race ./internal/engine ./internal/httpapi -count=1
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "All deterministic tests and race tests passed."
