$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Port = $env:PORT
if ([string]::IsNullOrWhiteSpace($Port)) { $Port = "18080" }
$Addr = $env:SHUFFLE_ADDR
if ([string]::IsNullOrWhiteSpace($Addr)) { $Addr = "127.0.0.1:$Port" }
$Db = $env:SHUFFLE_DB
if ([string]::IsNullOrWhiteSpace($Db)) { $Db = Join-Path $Root "data\engine.db" }
Set-Location $Root
go run ./cmd/server -addr $Addr -db $Db @args
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
