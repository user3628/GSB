param(
    [string]$Python = "python",
    [string]$Wheelhouse = (Join-Path (Split-Path -Parent $PSScriptRoot) "wheelhouse"),
    [switch]$ResetData
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "[1/6] Checking Python..."
& $Python -c "import sys; assert sys.version_info >= (3, 11), sys.version; print(sys.version)"
if ($LASTEXITCODE -ne 0) { throw "Python 3.11 or newer is required." }

$Venv = Join-Path $Root ".venv"
$VenvPython = Join-Path $Venv "Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    Write-Host "[2/6] Creating .venv..."
    & $Python -m venv $Venv
}
else {
    Write-Host "[2/6] Reusing .venv..."
}

if (-not (Test-Path $Wheelhouse)) {
    throw "Offline wheelhouse not found: $Wheelhouse. Place approved wheels there; no network installation is attempted."
}

Write-Host "[3/6] Installing from local wheels only..."
& $VenvPython -m pip install --no-index --find-links $Wheelhouse -r requirements.txt
if ($LASTEXITCODE -ne 0) { throw "Offline dependency installation failed." }

Write-Host "[4/6] Checking port 8000..."
& $VenvPython -m app.cli check-port --host 127.0.0.1 --port 8000
if ($LASTEXITCODE -ne 0) { throw "Port 8000 is not available." }

$Db = Join-Path $Root "data\scaler.db"
if ($ResetData -or -not (Test-Path $Db)) {
    Write-Host "[5/6] Initializing deterministic sample v1..."
    if ($ResetData) {
        & $VenvPython -m app.cli init --db $Db --policy "samples\policy_v1.json" --reset
    }
    else {
        & $VenvPython -m app.cli init --db $Db --policy "samples\policy_v1.json"
    }
    if ($LASTEXITCODE -ne 0) { throw "Sample policy initialization failed." }
}
else {
    Write-Host "[5/6] Existing database preserved."
}

Write-Host "[6/6] Running tests..."
& $VenvPython -m pytest -q
if ($LASTEXITCODE -ne 0) { throw "Tests failed." }

Write-Host ""
Write-Host "Setup complete. Start with: powershell -ExecutionPolicy Bypass -File scripts\run_windows.ps1"
