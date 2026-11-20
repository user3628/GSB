from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True, slots=True)
class ManagerConfig:
    data_dir: Path
    max_writing: int = 2
    max_checkpoints: int = 3
    min_retain_round_interval: int = 1
    # Delay is intentionally exposed for deterministic demos and tests.
    writer_delay_seconds: float = 0.0
    # Fault injection is enabled only by explicit CLI flags/test configuration.
    fault_crash_after_temp_files: bool = False
    fault_crash_delay_seconds: float = 0.8
    fault_corrupt_after_temp_write: bool = False

    def __post_init__(self) -> None:
        if self.max_writing < 1:
            raise ValueError("max_writing must be >= 1")
        if self.max_checkpoints < 1:
            raise ValueError("max_checkpoints must be >= 1")
        if self.min_retain_round_interval < 0:
            raise ValueError("min_retain_round_interval must be >= 0")
        if self.writer_delay_seconds < 0:
            raise ValueError("writer_delay_seconds must be >= 0")

    @property
    def database_path(self) -> Path:
        return self.data_dir / "checkpoints.db"

    @property
    def checkpoints_dir(self) -> Path:
        return self.data_dir / "checkpoints"

    @property
    def temp_dir(self) -> Path:
        return self.data_dir / "tmp"

    @property
    def intents_dir(self) -> Path:
        return self.data_dir / "intents"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "ManagerConfig":
        values = os.environ if env is None else env
        return cls(
            data_dir=Path(values.get("CKPT_DATA_DIR", "./checkpoint-data")).resolve(),
            max_writing=int(values.get("CKPT_MAX_WRITING", "2")),
            max_checkpoints=int(values.get("CKPT_MAX_CHECKPOINTS", "3")),
            min_retain_round_interval=int(values.get("CKPT_MIN_RETAIN_INTERVAL", "1")),
            writer_delay_seconds=float(values.get("CKPT_WRITER_DELAY_SECONDS", "0")),
            fault_crash_after_temp_files=values.get("CKPT_FAULT_CRASH_AFTER_TEMP", "0") == "1",
            fault_crash_delay_seconds=float(values.get("CKPT_FAULT_CRASH_DELAY_SECONDS", "0.8")),
            fault_corrupt_after_temp_write=values.get("CKPT_FAULT_CORRUPT_AFTER_TEMP", "0") == "1",
        )

