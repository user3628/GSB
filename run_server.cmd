@echo off
setlocal
cd /d "%~dp0"
python -m lease_lock.server --db runtime\leases.db --host 127.0.0.1 --port 8765 %*
endlocal

