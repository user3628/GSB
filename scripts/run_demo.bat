@echo off
rem Offline end-to-end demo: full build, stop/resume, incremental rebuild, verify.
cd /d "%~dp0\.."
set PYTHON=python
if exist .venv\Scripts\python.exe set PYTHON=.venv\Scripts\python.exe
%PYTHON% -m incseg --workspace demo_workspace demo --with-http
