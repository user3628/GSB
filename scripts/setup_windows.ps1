$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    throw "Python Launcher (py.exe) is required. Install Python 3.11 from python.org first."
}

py -3.11 -m venv .venv
& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install dnslib==0.9.26 pytest==9.2.0
& .\.venv\Scripts\python.exe -c "import sys, pytest; print('python', sys.version); print('pytest', pytest.__version__)"

Write-Host "Environment ready: $Root\.venv"
