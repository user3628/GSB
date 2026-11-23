param(
    [int]$Port = 18080,
    [switch]$KeepTemp
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

Write-Host "Go version:"
go version
if ($LASTEXITCODE -ne 0) { throw "go is not available on PATH" }

$demoArgs = @("./cmd/demo", "-port", "$Port")
if ($KeepTemp) { $demoArgs += "-keep-temp" }

& go run @demoArgs
exit $LASTEXITCODE
