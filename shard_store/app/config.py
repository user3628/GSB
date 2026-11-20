"""Runtime configuration. Overridable via environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "shard_store.db"
DEFAULT_SAMPLES_DIR = PROJECT_ROOT / "samples"

# Part numbering is 1-based and bounded like common object-storage conventions.
MIN_PART_NUMBER = 1
MAX_PART_NUMBER = 10000
DEFAULT_MIN_PART_SIZE = 32
DEFAULT_MAX_PART_SIZE = 5 * 1024 * 1024
# The last part is allowed to be smaller; size constraints are documented per API.
SQLITE_BUSY_TIMEOUT_MS = 5000


@dataclass(frozen=True)
class Settings:
    db_path: Path
    samples_dir: Path
    min_part_size_default: int = DEFAULT_MIN_PART_SIZE
    max_part_size_default: int = DEFAULT_MAX_PART_SIZE
    busy_timeout_ms: int = SQLITE_BUSY_TIMEOUT_MS

    @staticmethod
    def from_env() -> "Settings":
        db_path = Path(os.environ.get("SHARD_STORE_DB", str(DEFAULT_DB_PATH)))
        samples_dir = Path(os.environ.get("SHARD_STORE_SAMPLES", str(DEFAULT_SAMPLES_DIR)))
        return Settings(db_path=db_path, samples_dir=samples_dir)
