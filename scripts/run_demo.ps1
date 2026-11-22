param(
    [string]$Venv = ".venv",
    [string]$Workdir = "artifacts/demo"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$Python = Join-Path $Venv "Scripts\python.exe"
if (-not (Test-Path $Python)) {
    throw "Virtual environment not found. Run scripts/setup_windows.ps1 first."
}
& $Python -m dns_workbench demo --workdir $Workdir
exit $LASTEXITCODE

