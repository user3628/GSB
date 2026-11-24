$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$env:GOPROXY = "off"
$env:GOSUMDB = "off"

Write-Host "Go version: $(go version)"
go test ./...
New-Item -ItemType Directory -Force -Path bin | Out-Null
go build -trimpath -ldflags "-s -w" -o bin\driftctl.exe .\cmd\driftctl
Write-Host "Built bin\driftctl.exe"
