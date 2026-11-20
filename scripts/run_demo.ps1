$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Root
$env:WORKBENCH_ALLOW_CRASH = "1"
& .\.venv\Scripts\python.exe scripts\run_demo.py
