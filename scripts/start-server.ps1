param(
    [int]$Port = 0,
    [string]$DataDir = ""
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

if (-not $env:CACHE_ADDR) {
    $env:CACHE_ADDR = "127.0.0.1:$Port"
}
if (-not $env:CACHE_DIR) {
    if ($DataDir) {
        $env:CACHE_DIR = [System.IO.Path]::GetFullPath($DataDir)
    } else {
        $env:CACHE_DIR = Join-Path $repoRoot ".cache-data"
    }
}
if (-not $env:CACHE_CAPACITY_BYTES) {
    $env:CACHE_CAPACITY_BYTES = "1073741824"
}

Write-Host "Starting CI artifact cache on $env:CACHE_ADDR (port 0 selects a free loopback port)"
go run ./cmd/cachectl serve
