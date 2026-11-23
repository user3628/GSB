# 网关权重调度与并行探测工作台 - Windows 本地回环演示
# 要求：Go 1.22+，仅访问 127.0.0.1，不使用容器、不访问外部网络目标。
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$env:CGO_ENABLED = "0"
Write-Host "== go version ==" -ForegroundColor Cyan
go version

Write-Host "== unit tests (pure Go, CGO disabled) ==" -ForegroundColor Cyan
go test ./...

Write-Host "== end-to-end loopback demo ==" -ForegroundColor Cyan
go run ./cmd/demo
if ($LASTEXITCODE -ne 0) {
    Write-Error "demo failed with exit code $LASTEXITCODE"
    exit $LASTEXITCODE
}
Write-Host "demo finished; SQLite and intent log are under .\data" -ForegroundColor Green
