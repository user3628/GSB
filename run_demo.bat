@echo off
REM Deterministic local demo for the checkpoint / WAL replay component.
python --version
pip install -r requirements.txt
python scripts\generate_samples.py
python scripts\run_demo.py
echo.
echo Report written to artifacts\demo_report.json
