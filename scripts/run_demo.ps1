$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not (Test-Path .\.venv\Scripts\python.exe)) {
    throw "Missing .venv. Run scripts\setup_windows.ps1 first."
}

& .\.venv\Scripts\python.exe -m pytest -q
& .\.venv\Scripts\python.exe scripts\demo.py
