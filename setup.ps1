$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = "1"

if (-not (Test-Path ".venv")) {
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) {
        & py -3 -m venv .venv
    } else {
        & python -m venv .venv
    }
}

& .\.venv\Scripts\python.exe -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)"
if ($LASTEXITCODE -ne 0) {
    throw "Python 3.11 or newer is required."
}

& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -r requirements.txt
& .\.venv\Scripts\python.exe scripts\make_samples.py

Write-Host "Environment ready. Run: .\run.ps1"

