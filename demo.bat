@echo off
setlocal
if not exist .venv\Scripts\python.exe (
  echo [ERROR] .venv is missing. Run setup.bat first.
  exit /b 1
)
.venv\Scripts\python.exe scripts\demo.py
