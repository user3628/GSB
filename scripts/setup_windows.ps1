$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

if (Get-Command py -ErrorAction SilentlyContinue) {
    py -3.11 -m venv .venv
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    python -m venv .venv
} else {
    throw "Python 3.11+ was not found. Install it and ensure py/python is on PATH."
}

& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt

Write-Host "Setup complete. Run: .\scripts\run_demo.ps1"

