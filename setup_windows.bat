@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv311\Scripts\python.exe" (
  echo [1/3] Creating Python 3.11 virtual environment...
  py -3.11 -m venv .venv311
  if errorlevel 1 (
    echo py -3.11 failed; trying python on PATH...
    python -m venv .venv311
    if errorlevel 1 (
      echo Python 3.11 was not found. Install Python 3.11 and retry setup_windows.bat.
      exit /b 1
    )
  )
)

".venv311\Scripts\python.exe" -c "import sys; raise SystemExit(0 if sys.version_info[:2] == (3, 11) else 1)"
if errorlevel 1 (
  echo .venv311 does not use Python 3.11. Remove it and rerun setup_windows.bat with Python 3.11 installed.
  exit /b 1
)

echo [2/3] Installing locked dependencies...
".venv311\Scripts\python.exe" -m pip install --upgrade pip
".venv311\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 exit /b 1

echo [3/3] Generating deterministic samples and running acceptance demo...
".venv311\Scripts\python.exe" -m image_workbench generate-sample --out data
if errorlevel 1 exit /b 1
".venv311\Scripts\python.exe" -m image_workbench demo
if errorlevel 1 exit /b 1

echo.
echo Setup and verification complete.
echo Start the local UI with: run_server.bat
endlocal

