$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "python was not found in PATH. Install Python 3.11+ and enable the Python launcher option."
}

$version = python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
$parts = $version.Split(".")
if ([int]$parts[0] -lt 3 -or ([int]$parts[0] -eq 3 -and [int]$parts[1] -lt 11)) {
    throw "Python 3.11+ is required; found $version"
}

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    python -m venv .venv
}

& ".venv\Scripts\python.exe" -m pip install --upgrade pip
& ".venv\Scripts\python.exe" -m pip install -r requirements-dev.txt
& ".venv\Scripts\python.exe" -m sqlplan_engine doctor
& ".venv\Scripts\python.exe" -m sqlplan_engine init

Write-Host "Setup complete. Activate with: .\.venv\Scripts\Activate.ps1"

