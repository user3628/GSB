$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
& .\.venv\Scripts\python.exe scripts\run_experiment.py --output artifacts\verification_report.json
& .\.venv\Scripts\python.exe -m pytest -q
