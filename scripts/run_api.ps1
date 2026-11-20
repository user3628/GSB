$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root
$env:CONSISTENCY_DB = Join-Path $root "var\consistency.sqlite3"
& (Join-Path $root ".venv\Scripts\python.exe") -m uvicorn app.api:app --host 127.0.0.1 --port 8000
