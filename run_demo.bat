@echo off
setlocal
py -3 -m pip install -r requirements.txt
if errorlevel 1 exit /b 1
py -3 scripts\generate_samples.py --out samples
if errorlevel 1 exit /b 1
py -3 -m pytest -q
if errorlevel 1 exit /b 1
py -3 -m embedding_gather demo
