param(
    [string]$Python = "py -3",
    [string]$Venv = ".venv",
    [string]$Wheelhouse = "wheelhouse",
    [switch]$AllowNetwork
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "Checking Python..."
$VersionText = & cmd /c "$Python -c ""import sys; print('%d.%d' % sys.version_info[:2])"""
$Version = [version]$VersionText.Trim()
if ($Version -lt [version]"3.11") {
    throw "Python 3.11 or newer is required; found $VersionText"
}

if (-not (Test-Path $Venv)) {
    Write-Host "Creating virtual environment at $Venv (system packages visible for offline installs)..."
    & cmd /c "$Python -m venv --system-site-packages $Venv"
    if ($LASTEXITCODE -ne 0) { throw "venv creation failed" }
}

$VenvPython = Join-Path $Venv "Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    throw "virtual environment Python not found: $VenvPython"
}

Write-Host "Installing dependencies..."
if (Test-Path $Wheelhouse) {
    & $VenvPython -m pip install --no-index --find-links $Wheelhouse -r requirements.txt
} elseif ($AllowNetwork) {
    & $VenvPython -m pip install -r requirements.txt
} else {
    Write-Host "No local wheelhouse found. Checking installed system packages via --system-site-packages..."
    & $VenvPython -c "import fastapi, uvicorn, dnslib, pytest, httpx; print('system packages available')"
}
if ($LASTEXITCODE -ne 0) { throw "dependency installation or verification failed" }

& $VenvPython -m pip install -e . --no-deps --no-build-isolation
if ($LASTEXITCODE -ne 0) { throw "editable package install failed" }

Write-Host "Running tests..."
& $VenvPython -m pytest
if ($LASTEXITCODE -ne 0) { throw "tests failed" }

Write-Host "Ready. Start with: $VenvPython -m dns_workbench serve --config config.example.json"

