$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
& ".\.venv\Scripts\python.exe" -m pytest
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
& ".\.venv\Scripts\python.exe" -m tools.verify_samples
exit $LASTEXITCODE

