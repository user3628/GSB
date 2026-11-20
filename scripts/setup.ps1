$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path $PSScriptRoot -Parent
Set-Location $ProjectRoot

$pythonVersion = python -c "import sys; print('.'.join(map(str, sys.version_info[:3])))"
$pythonMajorMinor = python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
if ([version]$pythonMajorMinor -lt [version]"3.11") {
    throw "Python 3.11 or newer is required; found $pythonVersion"
}

python -m venv .venv
& .\.venv\Scripts\python.exe -m pip install --upgrade pip
& .\.venv\Scripts\python.exe -m pip install -r requirements.lock.txt
& .\.venv\Scripts\python.exe -c "import fastapi, uvicorn, httpx, pytest, sqlite3, sys; print('python', sys.version); print('fastapi', fastapi.__version__); print('uvicorn', uvicorn.__version__); print('httpx', httpx.__version__); print('pytest', pytest.__version__); print('sqlite', sqlite3.sqlite_version)"
