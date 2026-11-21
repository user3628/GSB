$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

$python = Get-Command py -ErrorAction SilentlyContinue
if ($python) {
  py -3.11 -m venv .venv
} else {
  python -m venv .venv
}

& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt

Write-Host ""
Write-Host "Environment ready. Run:"
Write-Host "  .\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 18080"
Write-Host "  .\.venv\Scripts\python.exe scripts\run_demo.py"
