@echo off
setlocal
cd /d "%~dp0\.."

where py >nul 2>nul
if %ERRORLEVEL% EQU 0 (
  py -3.11 -m venv .venv
  if errorlevel 1 py -3 -m venv .venv
) else (
  python -m venv .venv
)
if errorlevel 1 exit /b 1

".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 exit /b 1
".venv\Scripts\python.exe" -m pip install -e ".[test]"
if errorlevel 1 exit /b 1
".venv\Scripts\python.exe" -m pytest -q
exit /b %ERRORLEVEL%

