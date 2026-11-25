[CmdletBinding()]
param(
    [string]$Samples = "samples",
    [string]$DbPath = ".runtime/verify.db",
    [string]$ReportPath = "artifacts/verification-report.json",
    [switch]$SkipTests,
    [switch]$NoForceKill
)

$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path (Join-Path $PSScriptRoot ".."))

Write-Host "==> Go version"
go version

if (-not $SkipTests) {
    Write-Host "==> go test ./..."
    go test ./... -count=1
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

New-Item -ItemType Directory -Force -Path "bin", ".runtime", "artifacts" | Out-Null

Write-Host "==> Build bin/edge-dist.exe"
$env:CGO_ENABLED = "0"
go build -o "bin/edge-dist.exe" "./cmd/edge-dist"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$forceKillArg = if ($NoForceKill) { "-force-kill=false" } else { "-force-kill=true" }
Write-Host "==> Deterministic end-to-end verification"
& ".\bin\edge-dist.exe" verify `
    --samples $Samples `
    --db $DbPath `
    --output $ReportPath `
    $forceKillArg
exit $LASTEXITCODE
