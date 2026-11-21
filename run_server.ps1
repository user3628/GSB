param([int]$Port = 8080)
$ErrorActionPreference = "Stop"
if (Test-Path .\.venv\Scripts\Activate.ps1) { .\.venv\Scripts\Activate.ps1 }
python run_server.py --host 127.0.0.1 --port $Port
