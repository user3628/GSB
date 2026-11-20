$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

if (Get-Command py -ErrorAction SilentlyContinue) {
    py -3.11 -m venv .venv
    if ($LASTEXITCODE -ne 0) {
        py -3 -m venv .venv
    }
} else {
    python -m venv .venv
}
& ".venv\Scripts\python.exe" -m pip install --upgrade pip
& ".venv\Scripts\python.exe" -m pip install -e ".[test]"
& ".venv\Scripts\python.exe" -m pytest -q

