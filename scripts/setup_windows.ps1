$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $PSScriptRoot)

Write-Host "== Python version check =="
python --version
if ($LASTEXITCODE -ne 0) {
    throw "Python 3.11+ is required and python must be on PATH"
}

Write-Host "== Creating virtual environment =="
python -m venv .venv

Write-Host "== Installing dependencies =="
& .\.venv\Scripts\python.exe -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed" }
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw "dependency install failed" }

Write-Host "== Generating deterministic local shards =="
& .\.venv\Scripts\python.exe scripts\generate_samples.py --output data\seed_20261122 --seed 20261122
if ($LASTEXITCODE -ne 0) { throw "sample generation failed" }

Write-Host "== Checking loopback port 8765 =="
& .\.venv\Scripts\python.exe scripts\check_port.py --host 127.0.0.1 --port 8765
if ($LASTEXITCODE -ne 0) {
    Write-Error "Port 8765 is occupied. Stop the conflicting service, then rerun this script."
}

Write-Host "Setup complete. Start with: .\.venv\Scripts\python.exe -m mpc.server --port 8765"
