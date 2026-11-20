from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _as_bool(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    db_path: Path
    volume_id: str = "vol0"
    block_count: int = 8
    fault_injection: bool = False
    initial_content_prefix: str = "GENESIS"

    @classmethod
    def from_env(cls) -> "Settings":
        repo_root = Path(__file__).resolve().parent.parent
        default_db = repo_root / "data" / "cow_snapshots.db"
        raw_db = os.getenv("COW_DB_PATH")
        return cls(
            db_path=Path(raw_db).expanduser().resolve() if raw_db else default_db,
            volume_id=os.getenv("COW_VOLUME_ID", "vol0"),
            block_count=int(os.getenv("COW_BLOCK_COUNT", "8")),
            fault_injection=_as_bool(os.getenv("COW_FAULT_INJECTION")),
            initial_content_prefix=os.getenv("COW_INITIAL_PREFIX", "GENESIS"),
        )

    def initial_content(self, block_no: int) -> bytes:
        return f"{self.initial_content_prefix}:{block_no:04d}".encode("utf-8")

