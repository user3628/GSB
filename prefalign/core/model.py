"""Preference sample data model."""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Optional

REQUIRED_FIELDS = ("pair_id", "prompt", "chosen", "rejected", "source", "lang")


@dataclass(frozen=True)
class Sample:
    pair_id: str
    prompt: str
    chosen: str
    rejected: str
    source: str
    lang: str
    consistency: Optional[float] = None
    shard: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict[str, Any], shard: str | None = None) -> "Sample":
        return cls(
            pair_id=str(data["pair_id"]),
            prompt="" if data.get("prompt") is None else str(data["prompt"]),
            chosen="" if data.get("chosen") is None else str(data["chosen"]),
            rejected="" if data.get("rejected") is None else str(data["rejected"]),
            source=str(data.get("source", "unknown")),
            lang=str(data.get("lang", "unk")),
            consistency=data.get("consistency"),
            shard=shard,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
