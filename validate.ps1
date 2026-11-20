param(
    [string]$Output = "artifacts\validation\validation_report.json"
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    throw "Virtual environment is missing. Run .\setup.ps1 first."
}

& $python scripts\run_validation.py --output $Output
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

