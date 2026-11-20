@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv311\Scripts\python.exe" (
  echo Run setup_windows.bat first.
  exit /b 1
)
echo Starting http://127.0.0.1:8000
start "" "http://127.0.0.1:8000"
".venv311\Scripts\python.exe" -m uvicorn image_workbench.api:app --host 127.0.0.1 --port 8000
endlocal

