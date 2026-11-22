@echo off
setlocal
cd /d %~dp0
python -m blockgemm generate --out samples --force
if errorlevel 1 exit /b %errorlevel%
python -m blockgemm demo --manifest samples\manifest.json --out runs\demo
if errorlevel 1 exit /b %errorlevel%
endlocal
