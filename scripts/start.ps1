$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
if (-not (Test-Path ".venv\Scripts\python.exe")) {
    throw "Virtual environment missing. Run .\scripts\setup.ps1 first."
}
if (-not $env:DIAGLAB_DB) {
    $env:DIAGLAB_DB = Join-Path $Root "data\diaglab.sqlite3"
}
$Port = if ($env:DIAGLAB_PORT) { $env:DIAGLAB_PORT } else { "8000" }
& ".venv\Scripts\python.exe" -m uvicorn diaglab.app:app --host 127.0.0.1 --port $Port
