$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
go run ./cmd/verify @args
exit $LASTEXITCODE
