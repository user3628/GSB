param(
    [string]$Samples = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not $Samples) {
    $Samples = Join-Path $repoRoot "samples\entries.json"
}

go run ./cmd/cachectl verify --samples $Samples
exit $LASTEXITCODE
