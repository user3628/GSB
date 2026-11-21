from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    database_path: Path
    samples_dir: Path
    auto_seed: bool = True
    auto_recover: bool = True
    throttle_rejection_threshold: int = 3
    allow_chaos: bool = False

    @classmethod
    def from_env(cls) -> "Settings":
        root = Path(__file__).resolve().parent.parent
        db_path = Path(os.getenv("DATABASE_PATH", str(root / "data" / "gateway.db")))
        samples_dir = Path(os.getenv("SAMPLES_DIR", str(root / "samples")))
        return cls(
            database_path=db_path,
            samples_dir=samples_dir,
            auto_seed=os.getenv("AUTO_SEED", "1") not in {"0", "false", "False"},
            auto_recover=os.getenv("AUTO_RECOVER", "1") not in {"0", "false", "False"},
            throttle_rejection_threshold=int(os.getenv("THROTTLE_REJECTION_THRESHOLD", "3")),
            allow_chaos=os.getenv("ALLOW_CHAOS", "0") in {"1", "true", "True"},
        )
