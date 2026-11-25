[CmdletBinding()]
param(
    [string]$WorkDir = "",
    [string]$ReportPath = "artifacts\verification-report.json"
)

$ErrorActionPreference = "Stop"
$RootDir = Split-Path -Parent $PSScriptRoot
Set-Location $RootDir

New-Item -ItemType Directory -Force -Path "bin", "artifacts" | Out-Null

if ([string]::IsNullOrWhiteSpace($WorkDir)) {
    $stamp = [DateTimeOffset]::UtcNow.ToString("yyyyMMddHHmmssfff")
    $WorkDir = Join-Path ([System.IO.Path]::GetTempPath()) "drift-engine-verify-$stamp"
}
New-Item -ItemType Directory -Force -Path $WorkDir | Out-Null

Write-Host "==> go version"
go version
if ($LASTEXITCODE -ne 0) { throw "go version failed" }

Write-Host "==> go test ./..."
go test ./...
if ($LASTEXITCODE -ne 0) { throw "go test failed" }

Write-Host "==> go vet ./..."
go vet ./...
if ($LASTEXITCODE -ne 0) { throw "go vet failed" }

Write-Host "==> build drift-engine.exe"
$ExePath = Join-Path $RootDir "bin\drift-engine.exe"
go build -o $ExePath .\cmd\drift-engine
if ($LASTEXITCODE -ne 0) { throw "go build failed" }

$AbsoluteReport = if ([System.IO.Path]::IsPathRooted($ReportPath)) {
    $ReportPath
} else {
    Join-Path $RootDir $ReportPath
}
$ReportDirectory = Split-Path -Parent $AbsoluteReport
New-Item -ItemType Directory -Force -Path $ReportDirectory | Out-Null

Write-Host "==> deterministic acceptance checks"
Write-Host "    workdir: $WorkDir"
Write-Host "    report:  $AbsoluteReport"
& $ExePath verify -exe $ExePath -workdir $WorkDir -report $AbsoluteReport
if ($LASTEXITCODE -ne 0) { throw "drift-engine verify failed" }

Write-Host "==> verification passed"
