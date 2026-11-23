param(
    [string]$Config = "samples\config.json",
    [string]$Listen = "127.0.0.1:18080",
    [string]$Database = ""
)
$ErrorActionPreference = "Stop"
$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $Root
$args = @("serve", "--config", $Config, "--listen", $Listen)
if ($Database -ne "") {
    $args += @("--db", $Database)
}
go run ./cmd/cdnlab @args
