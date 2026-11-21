$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    throw "Python Launcher 'py' was not found. Install Python 3.11 and enable the launcher."
}

& py -3.11 --version
if ($LASTEXITCODE -ne 0) { throw "Python 3.11 is required." }

if (-not (Test-Path ".venv")) {
    & py -3.11 -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw "Could not create .venv" }
}

& .\.venv\Scripts\python.exe -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw "pip upgrade failed" }
& .\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
if ($LASTEXITCODE -ne 0) { throw "dependency installation failed" }
& .\.venv\Scripts\python.exe scripts\build_fixtures.py
if ($LASTEXITCODE -ne 0) { throw "fixture build failed" }
& .\.venv\Scripts\python.exe -m pytest -q
if ($LASTEXITCODE -ne 0) { throw "tests failed" }

Write-Host "Setup complete. Run run_demo.bat or run_server.bat."
