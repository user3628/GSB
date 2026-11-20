param(
    [int]$Port = 8765,
    [string]$Db = "data\replication_guard.sqlite3"
)

$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = "1"
$python = if (Test-Path ".venv\Scripts\python.exe") { ".venv\Scripts\python.exe" } else { "python" }

& $python -m replication_guard --host 127.0.0.1 --port $Port --db $Db

