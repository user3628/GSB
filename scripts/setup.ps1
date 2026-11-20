$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "Creating .venv..."
python -m venv .venv

$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    throw "Virtual environment creation failed: $Python not found"
}

& $Python -c "import sys; assert sys.version_info >= (3, 11), sys.version"
& $Python -m pip install --upgrade pip
& $Python -m pip install -r requirements.txt

Write-Host "Environment ready: $Python"

