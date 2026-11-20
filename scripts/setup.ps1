$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $Root

Write-Host "[1/3] Locating Python 3.11+"
$VenvPython = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    $ForeignVenv = Test-Path (Join-Path $Root ".venv")
    if ($ForeignVenv) {
        Write-Host "Removing an existing non-Windows virtual environment at .venv"
        Remove-Item -Recurse -Force (Join-Path $Root ".venv")
    }
    $PyLauncher = Get-Command py -ErrorAction SilentlyContinue
    if ($PyLauncher) {
        & py -3.11 -m venv .venv
        if ($LASTEXITCODE -ne 0) {
            & py -3 -m venv .venv
        }
    } else {
        python -m venv .venv
    }
}

Write-Host "[2/3] Verifying interpreter version"
& $VenvPython -c "import sys; assert sys.version_info >= (3, 11), sys.version; print(sys.version)"

Write-Host "[3/3] Installing local dependencies"
& $VenvPython -m pip install --upgrade pip
& $VenvPython -m pip install -r requirements.txt

Write-Host "Setup complete. Run: powershell -ExecutionPolicy Bypass -File scripts\run_server.ps1"
