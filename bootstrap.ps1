[CmdletBinding()]
param(
    [string]$PythonExe = "",
    [switch]$SkipDemo
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

if (-not (Test-Path ".venv")) {
    if ($PythonExe) {
        & $PythonExe -m venv .venv
    }
    elseif (Get-Command py -ErrorAction SilentlyContinue) {
        & py -3.11 -m venv .venv
    }
    elseif (Get-Command python3.11 -ErrorAction SilentlyContinue) {
        & python3.11 -m venv .venv
    }
    else {
        throw "Python 3.11 was not found. Install it or pass -PythonExe C:\Path\python.exe"
    }
}

$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    $Python = Join-Path $Root ".venv/bin/python"
}

$VersionCheck = & $Python -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)"
if ($LASTEXITCODE -ne 0) {
    throw "The virtual environment must use Python 3.11 or newer."
}

& $Python -m ensurepip --upgrade | Out-Null
& $Python -m pip install --upgrade pip
& $Python -m pip install -r requirements.txt
& $Python -m pip install -e .
& $Python scripts/generate_samples.py
& $Python -m unittest discover -s tests -v
if (-not $SkipDemo) {
    & $Python scripts/demo_interop.py
}

Write-Host "Bootstrap and verification completed." -ForegroundColor Green
