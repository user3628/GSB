param(
    [string]$PythonCommand = "python"
)

$ErrorActionPreference = "Stop"
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

Write-Host "Checking Python 3.11+ using: $PythonCommand"
& $PythonCommand -c "import sys; assert sys.version_info >= (3, 11), 'Python 3.11 or newer is required'; print(sys.version)"
if ($LASTEXITCODE -ne 0) { throw "Python 3.11+ was not found." }

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    Write-Host "Creating .venv"
    & $PythonCommand -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw "Failed to create the virtual environment." }
}

if (-not (Test-Path "wheelhouse")) {
    throw "wheelhouse directory is missing; offline installation cannot continue."
}

$VenvPython = Join-Path $RepoRoot ".venv\Scripts\python.exe"
Write-Host "Installing pinned dependencies from the local wheelhouse"
& $VenvPython -m pip install --disable-pip-version-check --no-index --find-links wheelhouse -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw "Offline dependency installation failed." }

& $VenvPython -c "import sqlite3, fastapi, uvicorn, pytest; print('sqlite', sqlite3.sqlite_version); print('fastapi', fastapi.__version__); print('uvicorn', uvicorn.__version__); print('pytest', pytest.__version__)"
if ($LASTEXITCODE -ne 0) { throw "Environment smoke test failed." }

Write-Host "Environment ready. See README.md for run and test commands."

