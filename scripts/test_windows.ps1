param(
    [switch]$SkipDemo
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    throw "Missing .venv. Run scripts\setup_windows.ps1 first."
}

Write-Host "Running test suite"
& $VenvPython -m pytest -q
if ($LASTEXITCODE -ne 0) { throw "pytest failed." }

if (-not $SkipDemo) {
    Write-Host "Running forced-termination end-to-end demo"
    & $VenvPython scripts\e2e_demo.py
    if ($LASTEXITCODE -ne 0) { throw "End-to-end demo failed." }
}

Write-Host "All local validations passed."

