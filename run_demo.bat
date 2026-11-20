@echo off
setlocal
cd /d "%~dp0"
if not exist ".venv311\Scripts\python.exe" (
  echo Run setup_windows.bat first.
  exit /b 1
)
".venv311\Scripts\python.exe" -m image_workbench generate-sample --out data
if errorlevel 1 exit /b 1
".venv311\Scripts\python.exe" -m image_workbench demo
if errorlevel 1 exit /b 1
endlocal

