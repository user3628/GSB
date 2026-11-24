param(
    [string]$RunDir = (Join-Path "var" ("demo-" + (Get-Date -Format "yyyyMMdd-HHmmss")))
)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
New-Item -ItemType Directory -Force -Path (Join-Path $RunDir "samples") | Out-Null
go run ./cmd/walgroup generate -seed 42 -out (Join-Path $RunDir "samples/workload-42.json")
go run ./cmd/walgroup demo `
    -workload (Join-Path $RunDir "samples/workload-42.json") `
    -data (Join-Path $RunDir "engine") `
    -clean `
    -report (Join-Path $RunDir "demo-report.json") |
    Set-Content -Encoding utf8 (Join-Path $RunDir "demo-output.json")
if ($LASTEXITCODE -ne 0) { throw "demo failed" }
go run ./cmd/walgroup crash-demo `
    -data (Join-Path $RunDir "crash") `
    -report (Join-Path $RunDir "crash-report.json") |
    Set-Content -Encoding utf8 (Join-Path $RunDir "crash-output.json")
if ($LASTEXITCODE -ne 0) { throw "crash-demo failed" }
Write-Host "demo output: $RunDir/demo-output.json"
Write-Host "crash output: $RunDir/crash-output.json"
