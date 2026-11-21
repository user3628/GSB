@REM Windows local environment setup (Python 3.11+). Run from the repo root.
@echo off
where python >nul 2>nul
if errorlevel 1 (
  echo Python not found on PATH. Install Python 3.11+ first.
  exit /b 1
)
python -m venv .venv
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
echo.
echo Environment ready. Run tests with:
echo   python -m unittest discover -s tests -v
echo Run the demo with:
echo   python scripts\demo.py
