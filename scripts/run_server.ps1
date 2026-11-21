$ErrorActionPreference = "Stop"
& .\.venv\Scripts\python.exe -m prep_pipeline.cli serve --host 127.0.0.1 --port 8767
