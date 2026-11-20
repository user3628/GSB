@echo off
setlocal
where py >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python launcher "py" was not found. Install Python 3.11 x64 from python.org first.
  exit /b 1
)
py -3.11 --version >nul 2>nul
if errorlevel 1 (
  echo [ERROR] Python 3.11 is not registered with the py launcher.
  echo Install Python 3.11 x64 and enable "Add Python to PATH", then run this script again.
  exit /b 1
)
if not exist .venv\Scripts\python.exe (
  py -3.11 -m venv .venv
  if errorlevel 1 exit /b 1
)
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 exit /b 1
.venv\Scripts\python.exe -m pytest
if errorlevel 1 exit /b 1
echo.
echo Setup complete. Run run.bat or demo.bat.
