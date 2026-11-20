$ErrorActionPreference = "Stop"

Set-Location (Split-Path -Parent $PSScriptRoot)

Write-Host "== Python =="
python --version
python -m pip --version

Write-Host "== Initialize local paths =="
python scripts\init_environment.py

Write-Host "== Rebuild deterministic samples =="
python scripts\build_samples.py

Write-Host "== Tests =="
python -m pytest -q

Write-Host "== End-to-end HTTP demo =="
python scripts\run_demo.py

Write-Host "Verification complete. See artifacts\demo_report.json"

