# Windows one-time setup: Python 3.11+ required, no Docker / no external CA.
$ErrorActionPreference = "Stop"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
# Use local wheels when offline:
#   pip install --no-index --find-links .\wheels -r requirements.txt
pip install -r requirements.txt
python -m tls_workbench.cli init
