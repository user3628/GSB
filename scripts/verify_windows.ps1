param([int]$Port = 8000)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$VenvPython = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) { throw "Run scripts\setup_windows.ps1 first." }

Write-Host "Running unit/integration suite..."
& $VenvPython -m pytest -q
if ($LASTEXITCODE -ne 0) { throw "Test suite failed." }

Write-Host "Running live HTTP + forced-termination acceptance..."
& $VenvPython scripts\verify_acceptance.py --host 127.0.0.1 --port $Port
if ($LASTEXITCODE -ne 0) { throw "Acceptance scenario failed." }

Write-Host "Report: artifacts\acceptance_report.json"
