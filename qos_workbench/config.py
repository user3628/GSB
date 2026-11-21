from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SAMPLES_DIR = PROJECT_ROOT / "samples"
DEFAULT_DB_PATH = Path(os.environ.get("QOS_DB_PATH", PROJECT_ROOT / "data" / "qos.db"))
API_HOST = os.environ.get("QOS_HOST", "127.0.0.1")
API_PORT = int(os.environ.get("QOS_PORT", "18080"))

