$ErrorActionPreference = "Stop"
$env:PYTHONUTF8 = "1"
$python = if (Test-Path ".venv\Scripts\python.exe") { ".venv\Scripts\python.exe" } else { "python" }

& $python scripts\make_samples.py
& $python -m pytest -q
& $python scripts\kill_recovery_demo.py

