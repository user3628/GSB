# Deterministic end-to-end demo on Windows (generation, parallel/serial,
# stop/resume, corruption and business error injection).
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
& .\.venv\Scripts\python.exe scripts\run_demo.py data\workspace
