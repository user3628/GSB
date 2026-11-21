$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not (Test-Path ".venv")) {
    py -3.11 -m venv .venv
}

& .venv\Scripts\python.exe -m pip install --upgrade pip
& .venv\Scripts\python.exe -m pip install -r requirements-dev.txt
& .venv\Scripts\python.exe scripts\verify_environment.py
& .venv\Scripts\python.exe -m pytest -q

