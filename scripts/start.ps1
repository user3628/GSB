param(
    [string]$DbPath = "var\h2workbench.db",
    [int]$Port = 8765
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    & "$PSScriptRoot\bootstrap.ps1"
}

Write-Host "Starting loopback HTTP/2 diagnostics workbench at http://127.0.0.1:$Port"
Write-Host "SQLite: $DbPath"
& ".venv\Scripts\python.exe" -m h2workbench.server --db $DbPath --host 127.0.0.1 --port $Port
