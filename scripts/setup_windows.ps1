$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    throw "Python launcher 'py' not found. Install Python 3.11 or 3.12 first."
}

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    py -3.12 -m venv .venv
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Python 3.12 unavailable, trying Python 3.11..." -ForegroundColor Yellow
        py -3.11 -m venv .venv
    }
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path ".venv\Scripts\python.exe")) {
        throw "Unable to create venv with Python 3.12 or 3.11."
    }
}

& ".venv\Scripts\python.exe" -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed" }
& ".venv\Scripts\python.exe" -m pip install -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw "dependency installation failed" }
& ".venv\Scripts\python.exe" scripts\build_samples.py
if ($LASTEXITCODE -ne 0) { throw "sample generation failed" }
Write-Host "Environment ready. Run scripts\run_server.ps1" -ForegroundColor Green
