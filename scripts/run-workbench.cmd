@echo off
setlocal
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run-workbench.ps1" %*
exit /b %ERRORLEVEL%
