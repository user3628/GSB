param(
    [int]$Port = 8000
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    throw "Missing .venv. Run scripts\setup_windows.ps1 first."
}

$env:PYTHONPATH = $RepoRoot
& $VenvPython -m uvicorn app.main:app --host 127.0.0.1 --port $Port --workers 1

