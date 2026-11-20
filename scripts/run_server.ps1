$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Root
$env:WORKBENCH_DB_PATH = Join-Path $Root "var\workbench.db"
New-Item -ItemType Directory -Force -Path (Join-Path $Root "var") | Out-Null

Write-Host "Starting loopback workbench at http://127.0.0.1:8765"
Write-Host "Diagnostics page: http://127.0.0.1:8765/"
& .\.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8765
