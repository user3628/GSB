"""Initialise an empty component database.

Usage: python -m scripts.init_db [--reset]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import Settings  # noqa: E402
from app.db.connection import connect  # noqa: E402
from app.db.schema import SCHEMA_SQL  # noqa: E402


def init_db(reset: bool = False) -> Path:
    settings = Settings.from_env()
    if reset and settings.db_path.exists():
        for suffix in ("", "-wal", "-shm"):
            sidecar = Path(str(settings.db_path) + suffix)
            if sidecar.exists():
                sidecar.unlink()
    conn = connect(settings)
    try:
        conn.executescript(SCHEMA_SQL)
        tables = [
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
    finally:
        conn.close()
    print(f"Initialised database at {settings.db_path}")
    print("Tables:", ", ".join(tables))
    return settings.db_path


if __name__ == "__main__":
    init_db(reset="--reset" in sys.argv)
