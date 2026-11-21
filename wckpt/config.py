"""Component configuration."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Dict


@dataclass
class Config:
    # WAL segment rotation: rotate once the segment file exceeds this size.
    segment_max_bytes: int = 64 * 1024
    # Adaptive replay: below both thresholds use the precise one-by-one path.
    replay_record_threshold: int = 1000
    replay_dirty_page_threshold: int = 200
    # Batched path tuning.
    replay_batch_size: int = 500
    flush_group_size: int = 256
    # Incremental checkpoint background flusher.
    background_flush: bool = False
    background_flush_interval_ms: int = 200
    # Durability knobs (fsync of segment files / directories).
    fsync_wal: bool = True
    fsync_dirs: bool = True
    # SQLite synchronous mode: FULL (default, safest) / NORMAL / OFF.
    sqlite_synchronous: str = "FULL"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Config":
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in (data or {}).items() if k in known})

    @classmethod
    def from_json_file(cls, path: str) -> "Config":
        with open(path, "r", encoding="utf-8") as fh:
            return cls.from_dict(json.load(fh))

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
