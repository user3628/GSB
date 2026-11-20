$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
& ".venv\Scripts\python.exe" -m quant_audit --pretty demo --workspace demo_run --data-dir demo_data --report artifacts\demo_report.json --force --faults

