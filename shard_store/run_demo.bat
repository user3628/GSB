@echo off
REM Windows equivalent of run_demo.sh (no Docker required).
cd /d "%~dp0"
if exist "..\.venv\Scripts\activate.bat" call "..\.venv\Scripts\activate.bat"
python -m scripts.init_db --reset || exit /b 1
python -m pytest || exit /b 1
python scripts\demo_e2e.py || exit /b 1
