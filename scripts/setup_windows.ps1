param(
    [string]$WheelDir = ""
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

python --version
pip --version
python -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)"
if ($LASTEXITCODE -ne 0) { throw "Python 3.11 or newer is required" }

if (-not (Test-Path ".venv")) {
    python -m venv .venv
}
$Python = Join-Path (Get-Location) ".venv\Scripts\python.exe"
if ($WheelDir -ne "") {
    & $Python -m pip install --no-index --find-links $WheelDir -r requirements.txt
} else {
    & $Python -m pip install --upgrade pip
    & $Python -m pip install -r requirements.txt
}
& $Python -m pytest
Write-Host "Setup complete. Run scripts\run_demo.ps1"

