$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
if (-not (Test-Path ".venv")) {
    if ($env:PYTHON) {
        & $env:PYTHON -m venv .venv
    } elseif (Get-Command py -ErrorAction SilentlyContinue) {
        py -3 -m venv .venv
    } else {
        python -m venv .venv
    }
}
& ".venv\Scripts\python.exe" -c "import sys; assert sys.version_info >= (3, 11), 'Python 3.11+ required'"
& ".venv\Scripts\python.exe" -m pip install --upgrade pip
& ".venv\Scripts\python.exe" -m pip install -r requirements.txt
& ".venv\Scripts\python.exe" scripts\make_samples.py | Out-Null
Write-Host "Setup complete. Run: .\scripts\start.ps1"
