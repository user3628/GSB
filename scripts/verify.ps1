$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Root
& .\.venv\Scripts\python.exe -m pytest -q
if ($LASTEXITCODE -ne 0) { throw "pytest failed" }
& .\.venv\Scripts\python.exe scripts\run_demo.py
if ($LASTEXITCODE -ne 0) { throw "demo failed" }
Write-Host "Verification complete. See artifacts\demo_results.json"
