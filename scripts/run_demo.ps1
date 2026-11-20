$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
python scripts/generate_samples.py
python scripts/e2e_demo.py
