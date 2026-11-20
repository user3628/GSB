param(
    [int]$Port = 8000,
    [string]$DataDir = "data",
    [int]$MinimumSamples = 100,
    [int]$ExactEventLimit = 10000,
    [int]$LateToleranceSeconds = 120
)

$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
$env:PYTHONUTF8 = "1"
$env:SLO_DATA_DIR = $DataDir
$env:SLO_DB_PATH = Join-Path $DataDir "slo.db"
$env:SLO_MINIMUM_SAMPLES = "$MinimumSamples"
$env:SLO_EXACT_EVENT_LIMIT = "$ExactEventLimit"
$env:SLO_LATE_TOLERANCE_SECONDS = "$LateToleranceSeconds"

Write-Host "Starting local SLI/SLO service on http://127.0.0.1:$Port"
Write-Host "SQLite data directory: $DataDir"
python -m uvicorn app:app --host 127.0.0.1 --port $Port --workers 1
