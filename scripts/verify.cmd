@echo off
setlocal
where powershell >nul 2>nul
if errorlevel 1 (
  echo PowerShell is required but was not found on PATH.
  exit /b 1
)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0verify.ps1" %*
exit /b %ERRORLEVEL%
