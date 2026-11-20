param(
    [string]$DbPath = "var\demo.db",
    [switch]$Keep
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    & "$PSScriptRoot\bootstrap.ps1"
}

$demoArgs = @("scripts\demo.py", "--db", $DbPath)
if ($Keep) { $demoArgs += "--keep" }
& ".venv\Scripts\python.exe" @demoArgs
