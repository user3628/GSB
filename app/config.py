from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
STATIC_DIR = PROJECT_ROOT / "app" / "static"
SAMPLES_DIR = PROJECT_ROOT / "samples"


def database_path() -> Path:
    raw = os.environ.get("WORKBENCH_DB_PATH")
    if raw:
        return Path(raw).expanduser().resolve()
    return PROJECT_ROOT / "var" / "workbench.db"


def allow_demo_crash() -> bool:
    return os.environ.get("WORKBENCH_ALLOW_CRASH", "0") == "1"


PRECISION_TOLERANCE = {
    "memory_abs_dev_bytes": 0,
    "latency_abs_dev_us": 5,
    "relative_dev_ppm": 5_000,
    "policy": "abs_dev <= absolute bound AND relative_dev <= relative bound",
}
