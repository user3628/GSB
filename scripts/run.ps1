param(
    [int]$Port = 8080,
    [string]$Database = "data\workbench.db",
    [switch]$NoSeed
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
go version | Out-Host
$argsList = @("-addr", "127.0.0.1:$Port", "-db", $Database, "-allow-crash")
if (-not $NoSeed) { $argsList += @("-seed", "samples/demo.json") }
& go run . @argsList
exit $LASTEXITCODE
