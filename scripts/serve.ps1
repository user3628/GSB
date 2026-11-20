# Start the local HTTP workbench on Windows. Requires the project .venv.
$Root = Split-Path -Parent $PSScriptRoot
$Host_ = if ($env:HOST) { $env:HOST } else { "127.0.0.1" }
$Port = if ($env:PORT) { $env:PORT } else { "8000" }
Set-Location $Root
& .\.venv\Scripts\python.exe -m uvicorn lora_bench.http_app:app --host $Host_ --port $Port --app-dir src
