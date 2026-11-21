$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

python --version
if (-not (Test-Path ".venv")) {
    python -m venv .venv
}

$python = Join-Path (Get-Location) ".venv\Scripts\python.exe"
if (Test-Path "wheelhouse") {
    & $python -m pip install --no-index --find-links wheelhouse -r requirements.txt
} else {
    & $python -m pip install -r requirements.txt
}

New-Item -ItemType Directory -Force data, demo_output | Out-Null
& $python -c "import sqlite3; print('sqlite3', sqlite3.sqlite_version)"
& $python -m pytest

Write-Host ""
Write-Host "Setup complete. Activate with: .\.venv\Scripts\Activate.ps1"
Write-Host "Run demo with:          python scripts/demo.py"
Write-Host "Run API with:           python -m uvicorn pdb_engine.api:app --host 127.0.0.1 --port 8000"

