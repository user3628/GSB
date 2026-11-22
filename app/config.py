from __future__ import annotations

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = ROOT / "data" / "engine.db"


def sqlite_url(value: str | None = None) -> str:
    raw = value or os.getenv("STREAM_AGG_DB") or str(DEFAULT_DB_PATH)
    if raw.startswith("sqlite:///"):
        return raw
    path = Path(raw).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{path}"


def database_path(url: str) -> Path | None:
    if not url.startswith("sqlite:///"):
        return None
    return Path(url.removeprefix("sqlite:///"))


def demo_crash_enabled() -> bool:
    return os.getenv("ENABLE_DEMO_CRASH", "0") == "1"
