$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root
& (Join-Path $root ".venv\Scripts\python.exe") (Join-Path $root "scripts\demo.py")
