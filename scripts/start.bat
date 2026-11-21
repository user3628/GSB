@echo off
setlocal
cd /d "%~dp0\.."

python -c "import sys; raise SystemExit(0 if sys.version_info >= (3,11) else 1)" >nul 2>&1
if errorlevel 1 (
  echo Python 3.11 or newer is required.
  exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
  echo Creating project virtual environment...
  python -m venv .venv
  if errorlevel 1 exit /b 1
)

call ".venv\Scripts\activate.bat"
python -m pip install --disable-pip-version-check -r requirements.txt
if errorlevel 1 exit /b 1

python scripts\check_ports.py --http-port 18765
if errorlevel 1 exit /b 1

echo.
echo Dashboard: http://127.0.0.1:18765
echo API docs:  http://127.0.0.1:18765/docs
echo.
python -m vgw serve --host 127.0.0.1 --port 18765 --db artifacts\vgw.db

