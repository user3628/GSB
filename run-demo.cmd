@echo off
setlocal
if "%~1"=="" (
  go run ./cmd/tokenforge-demo -out tokenforge-report.json
) else (
  go run ./cmd/tokenforge-demo %*
)
