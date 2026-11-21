$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Python = if ($env:PYTHON_BIN) { $env:PYTHON_BIN } else { "python" }

& $Python -c "import sys; assert sys.version_info >= (3, 10), 'Python 3.10 or newer is required'"
if (-not (Test-Path (Join-Path $Root ".venv"))) {
    & $Python -m venv (Join-Path $Root ".venv")
}
$VenvPython = Join-Path $Root ".venv\Scripts\python.exe"
& $VenvPython -m pip install --no-index --find-links (Join-Path $Root "wheelhouse") -e $Root pytest
Write-Host "Offline environment ready: $Root\.venv"

