$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $ProjectRoot

& .\.venv\Scripts\python.exe -m pytest
if ($LASTEXITCODE -ne 0) { throw "pytest failed" }
& .\.venv\Scripts\python.exe tools\demo.py
if ($LASTEXITCODE -ne 0) { throw "demo failed" }
