param(
  [string]$Config = "samples/config.json",
  [string]$DB = "",
  [string]$Addr = ""
)

$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path (Join-Path $PSScriptRoot ".."))

$arguments = @("run", "./cmd/workbench", "-config", $Config)
if ($DB) { $arguments += @("-db", $DB) }
if ($Addr) { $arguments += @("-addr", $Addr) }

Write-Host "Starting local workbench. Press Ctrl+C to stop."
& go @arguments
if ($LASTEXITCODE -ne 0) { throw "workbench exited with code $LASTEXITCODE" }
