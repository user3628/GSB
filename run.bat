@echo off
setlocal
if not exist .venv\Scripts\python.exe (
  echo [ERROR] .venv is missing. Run setup.bat first.
  exit /b 1
)
.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8000
