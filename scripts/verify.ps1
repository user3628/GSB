param(
  [switch]$WithDemo
)

$ErrorActionPreference = "Stop"
$root = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $root

function Assert-LastExit([string]$What) {
  if ($LASTEXITCODE -ne 0) {
    throw "$What failed with exit code $LASTEXITCODE"
  }
}

$go = Get-Command go -ErrorAction SilentlyContinue
if (-not $go) { throw "go was not found on PATH" }
$goVersion = (& go version)
$match = [regex]::Match($goVersion, "go(\d+)\.(\d+)")
if (-not $match.Success) { throw "cannot parse go version: $goVersion" }
$major = [int]$match.Groups[1].Value
$minor = [int]$match.Groups[2].Value
if ($major -lt 1 -or ($major -eq 1 -and $minor -lt 22)) {
  throw "Go 1.22 or newer is required; found $goVersion"
}
Write-Host "OK  $goVersion"

$gcc = Get-Command gcc -ErrorAction SilentlyContinue
if ($gcc) {
  Write-Host "OK  gcc available: $((& gcc --version | Select-Object -First 1))"
} else {
  Write-Host "INFO gcc not found; this project uses the pure-Go modernc.org/sqlite driver, so CGO is not required."
}

Write-Host "== go mod download (offline builds can reuse the module cache)"
& go mod download
Assert-LastExit "go mod download"

Write-Host "== go test ./..."
& go test ./... -count=1
Assert-LastExit "go test"

Write-Host "== go vet ./..."
& go vet ./...
Assert-LastExit "go vet"

Write-Host "== build Windows executable"
$previousCGO = $env:CGO_ENABLED
$env:CGO_ENABLED = "0"
New-Item -ItemType Directory -Force -Path ".build" | Out-Null
& go build -trimpath -o ".build\workbench.exe" ./cmd/workbench
Assert-LastExit "go build"
if ($null -eq $previousCGO) { Remove-Item Env:CGO_ENABLED } else { $env:CGO_ENABLED = $previousCGO }
Write-Host "OK  .build\workbench.exe"

if ($WithDemo) {
  & (Join-Path $PSScriptRoot "demo.ps1")
  Assert-LastExit "demo"
}

Write-Host "Verification complete. Loopback address: 127.0.0.1"
