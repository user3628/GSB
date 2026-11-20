@echo off
setlocal
cd /d "%~dp0\.."
".venv\Scripts\python.exe" -m quant_audit --pretty demo --workspace demo_run --data-dir demo_data --report artifacts\demo_report.json --force --faults
if errorlevel 1 exit /b 1
exit /b 0

