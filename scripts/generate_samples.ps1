$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Out = $env:OUT
if ([string]::IsNullOrWhiteSpace($Out)) { $Out = Join-Path $Root "data" }
Set-Location $Root
go run ./cmd/samplegen -out $Out @args
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
