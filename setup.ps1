param(
    [switch]$SkipTests,
    [switch]$SkipSamples
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "python was not found on PATH. Install Python 3.11+ and enable 'Add python.exe to PATH'."
}

$version = python -c "import sys; print('.'.join(map(str, sys.version_info[:2])))"
$parts = $version.Split('.')
if ([int]$parts[0] -lt 3 -or ([int]$parts[0] -eq 3 -and [int]$parts[1] -lt 11)) {
    throw "Python 3.11 or newer is required; found $version."
}

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    python -m venv .venv
}

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
& $python -m pip install --upgrade pip
& $python -m pip install -r requirements.txt

if (-not $SkipSamples) {
    & $python scripts\generate_samples.py
}
if (-not $SkipTests) {
    & $python -m pytest -q
}

Write-Host ""
Write-Host "Environment ready. Start the loopback-only service with:" -ForegroundColor Green
Write-Host "  .\run_service.ps1"
Write-Host "Run acceptance validation with:"
Write-Host "  .\validate.ps1"

