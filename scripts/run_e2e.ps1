$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
& .\.venv\Scripts\python.exe scripts\e2e.py
if ($LASTEXITCODE -ne 0) {
    throw "End-to-end verification failed with exit code $LASTEXITCODE"
}
