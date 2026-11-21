@echo off
setlocal
cd /d "%~dp0\.."

python -c "import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)" >nul 2>&1
if errorlevel 1 (
  echo Python 3.11 or newer is required.
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  python -m venv .venv
  if errorlevel 1 exit /b 1
)
call ".venv\Scripts\activate.bat"
python -m pip install --disable-pip-version-check -r requirements.txt
if errorlevel 1 exit /b 1

python scripts\check_ports.py --http-port 18765
if errorlevel 1 exit /b 1

python -m vgw demo --output artifacts\demo\current
if errorlevel 1 exit /b 1

echo.
echo Verification report: artifacts\demo\current\report.md
echo Recovery difference:  artifacts\demo\current\recovery_diff.json

