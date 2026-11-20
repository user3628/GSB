param(
    [string]$Db = "artifacts\msmq.sqlite3",
    [int]$Port = 8765,
    [switch]$EnableFailpoints
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    throw "Virtual environment is missing. Run .\setup.ps1 first."
}

$arguments = @(
    "scripts\start_service.py",
    "--host", "127.0.0.1",
    "--port", "$Port",
    "--db", $Db
)
if ($EnableFailpoints) {
    $arguments += "--enable-failpoints"
}
& $python @arguments

