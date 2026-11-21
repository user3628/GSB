@echo off
setlocal
cd /d "%~dp0"
python scripts\run_demo.py --root .
set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%

