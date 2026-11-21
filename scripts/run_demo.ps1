$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)
& ".venv\Scripts\python.exe" scripts\demo.py --output artifacts\demo_evidence.json
& ".venv\Scripts\python.exe" scripts\crash_and_recover.py
