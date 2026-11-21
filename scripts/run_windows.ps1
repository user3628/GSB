param(
    [int]$Port = 8000,
    [switch]$ResetData,
    [switch]$NoBrowser,
    [switch]$EnableFaultInjection
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root
$VenvPython = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) { throw "Run scripts\setup_windows.ps1 first." }

$Db = Join-Path $Root "data\scaler.db"
& $VenvPython -m app.cli check-port --host 127.0.0.1 --port $Port
if ($LASTEXITCODE -ne 0) { throw "Port $Port is not available." }

if ($ResetData -or -not (Test-Path $Db)) {
    if ($ResetData) {
        & $VenvPython -m app.cli init --db $Db --policy "samples\policy_v1.json" --reset
    }
    else {
        & $VenvPython -m app.cli init --db $Db --policy "samples\policy_v1.json"
    }
    if ($LASTEXITCODE -ne 0) { throw "Failed to initialize sample policy." }
}

if ($EnableFaultInjection) { $env:SCALE_ENABLE_FAULT_INJECTION = "1" }
if (-not $NoBrowser) {
    Start-Job -ScriptBlock { param($Url) Start-Sleep -Seconds 2; Start-Process $Url } -ArgumentList "http://127.0.0.1:$Port" | Out-Null
}
Write-Host "Scaling Arbiter UI: http://127.0.0.1:$Port"
Write-Host "OpenAPI docs:      http://127.0.0.1:$Port/docs"
& $VenvPython -m app.cli serve --db $Db --host 127.0.0.1 --port $Port
