param(
    [int]$Port = 0,
    [string]$DataDir = ""
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    throw "missing .venv; run scripts\setup_windows.ps1 first"
}
$ArgsList = @("scripts\run_server.py", "--port", "$Port")
if ($DataDir -ne "") {
    $ArgsList += @("--data-dir", $DataDir)
}
& $Python @ArgsList

