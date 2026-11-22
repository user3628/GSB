from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Settings:
    db_path: Path
    host: str = "127.0.0.1"
    port: int = 18080
    enable_failpoints: bool = False
    samples_dir: Path = ROOT / "samples"

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=Path(os.environ.get("WORKBENCH_DB", ROOT / "runtime" / "workbench.sqlite3")),
            host=os.environ.get("WORKBENCH_HOST", "127.0.0.1"),
            port=int(os.environ.get("WORKBENCH_PORT", "18080")),
            enable_failpoints=os.environ.get("WORKBENCH_ENABLE_FAILPOINTS", "0") == "1",
        )
