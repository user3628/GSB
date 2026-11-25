# Independent-process launcher for force-kill recovery verification on Windows.
# Usage: powershell -File scripts/run_server.ps1 [-Workdir .\workspace] [-Addr 127.0.0.1:8787]
param(
    [string]$Workdir = (Join-Path $PSScriptRoot "..\workspace"),
    [string]$Addr = "127.0.0.1:8787"
)
$root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
New-Item -ItemType Directory -Force -Path $Workdir | Out-Null
Set-Location $root
go run ./cmd/voxelrecon -workdir $Workdir -addr $Addr
