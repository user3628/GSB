[CmdletBinding()]
param(
    [int]$Items = 800,
    [int]$Queries = 120,
    [int]$Workers = 4,
    [int]$Concurrency = 4,
    [string]$Strategy = "weighted",
    [string]$DataDir = "data",
    [switch]$Inline
)

$ErrorActionPreference = "Stop"
$env:GOPROXY = "off"
$env:GOSUMDB = "off"
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
try {
    New-Item -ItemType Directory -Force -Path "bin" | Out-Null
    go build -o "bin\recallab.exe" ".\cmd\recallab"
    & ".\bin\recallab.exe" generate --out $DataDir --seed 20261125 --items $Items --queries $Queries --vector-dim 24 --strategy $Strategy
    $mode = if ($Inline) { "inline" } else { "process" }
    & ".\bin\recallab.exe" run --data $DataDir --db (Join-Path $DataDir "experiment.db") --out (Join-Path $DataDir "results.json") --workers $Workers --concurrency $Concurrency --mode $mode
    Write-Host "Results: $(Join-Path $DataDir 'results.json')"
    Write-Host "Database: $(Join-Path $DataDir 'experiment.db')"
}
finally {
    Pop-Location
}
