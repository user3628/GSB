param(
    [string]$WorkDir = ".tmp\demo",
    [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

Write-Host "== Recovery Coordinator / Windows end-to-end demo ==" -ForegroundColor Cyan
Write-Host "Repository: $repoRoot"
$goVersion = (& go version)
if ($LASTEXITCODE -ne 0) {
    throw "go was not found in PATH. Install Go 1.22 or newer first."
}
Write-Host "Toolchain:  $goVersion"
Write-Host "Ports:      127.0.0.1:19090 (probe endpoint), 127.0.0.1:19180/19181 (coordinator)"
Write-Host "WorkDir:    $WorkDir"
Write-Host ""

$arguments = @("run", "./cmd/demo", "-workdir", $WorkDir)
if ($SkipBuild) {
    $arguments += "-skip-build"
}

& go @arguments
if ($LASTEXITCODE -ne 0) {
    throw "The end-to-end demo failed. Inspect the newest directory under $WorkDir."
}

$latest = Get-ChildItem -Path $WorkDir -Directory | Sort-Object Name -Descending | Select-Object -First 1
if ($latest) {
    Write-Host ""
    Write-Host "Evidence: $($latest.FullName)\evidence.json" -ForegroundColor Green
    Get-ChildItem -Path $latest.FullName -Filter *.log | ForEach-Object {
        Write-Host "Log:      $($_.FullName)"
    }
}
