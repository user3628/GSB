$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
go test ./...
go run ./cmd/demo -seed 20261125 -events 64
