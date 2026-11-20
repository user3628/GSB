$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    throw "Virtual environment is missing. Run .\setup.ps1 first."
}
& $python scripts\generate_samples.py

