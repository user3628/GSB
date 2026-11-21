# PowerShell entry point (Windows local loopback, no network / containers).
python --version
pip install -r requirements.txt
python scripts/generate_samples.py
python scripts/run_demo.py
Write-Host "`nReport written to artifacts/demo_report.json"
