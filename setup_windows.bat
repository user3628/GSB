@echo off
setlocal
cd /d "%~dp0"

where py >nul 2>nul
if errorlevel 1 (
  echo Python Launcher 'py' was not found.
  echo Install Python 3.11 from https://www.python.org/downloads/windows/ and enable the launcher.
  exit /b 1
)

py -3.11 --version >nul 2>nul
if errorlevel 1 (
  echo Python 3.11 is required. Install it with: py -3.11
  exit /b 1
)

if not exist .venv (
  py -3.11 -m venv .venv
  if errorlevel 1 exit /b 1
)

call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
if errorlevel 1 exit /b 1
python -m pip install -r requirements-dev.txt
if errorlevel 1 exit /b 1
python scripts\build_fixtures.py
if errorlevel 1 exit /b 1
python -m pytest -q
if errorlevel 1 exit /b 1

echo.
echo Setup complete. Run run_demo.bat or run_server.bat.
exit /b 0
