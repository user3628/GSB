param(
    [string]$PythonCommand = "python"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not (Test-Path ".venv")) {
    & $PythonCommand -m venv .venv
    if ($LASTEXITCODE -ne 0) {
        throw "failed to create .venv with $PythonCommand"
    }
}

$Python = Join-Path $Root ".venv\Scripts\python.exe"
& $Python -m unittest discover -s tests -v
if ($LASTEXITCODE -ne 0) {
    throw "unit tests failed"
}

Write-Host ""
Write-Host "Environment ready: $Python"
Write-Host "Run demo:  .\.venv\Scripts\python.exe .\scripts\run_demo.py"
Write-Host "Run server: .\.venv\Scripts\python.exe .\scripts\run_server.py --port 0"

