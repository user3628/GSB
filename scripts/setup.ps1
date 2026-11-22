$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    Write-Host "Python launcher not found; installing Python 3.11 with winget..."
    winget install --id Python.Python.3.11 -e --accept-source-agreements --accept-package-agreements
}

py -3.11 -c "import sys; print(sys.version)"
if (-not (Test-Path ".venv\Scripts\python.exe")) {
    py -3.11 -m venv .venv
}

& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt
& .\.venv\Scripts\python.exe -m svc_registry.cli generate-samples
Write-Host "Environment ready. Run .\scripts\run_tests.ps1 and .\scripts\run_e2e.ps1"
