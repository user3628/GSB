@echo off
setlocal
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  echo .venv is missing. Run setup_windows.bat first.
  exit /b 1
)
call .venv\Scripts\activate.bat
python run.py serve --database artifacts\workbench.sqlite3 --sample-db data\slowquery_sample.sqlite3 %*
