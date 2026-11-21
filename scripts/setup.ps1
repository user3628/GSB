$ErrorActionPreference = "Stop"

if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
    throw "Python launcher 'py' was not found. Install Python 3.11 first."
}

py -3.11 -m venv .venv
& .\.venv\Scripts\python.exe -m pip install --upgrade pip setuptools wheel
& .\.venv\Scripts\python.exe -m pip install -e ".[dev]"

Write-Host "Creating deterministic local data directories..."
New-Item -ItemType Directory -Force artifacts | Out-Null

Write-Host "Initializing SQLite and submitting bundled policies..."
& .\.venv\Scripts\python.exe -m prep_pipeline.cli init
& .\.venv\Scripts\python.exe -m prep_pipeline.cli bootstrap

Write-Host "Setup complete. Run scripts\run_server.ps1, then open http://127.0.0.1:8767/docs"
