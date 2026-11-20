from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any


CURRENT_SCHEMA_VERSION = 2
METADATA_PREFIX = "[[WB-META]]"
HEADER_PREFIX = "[[WB-HEADER]]"


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def digest_value(value: Any) -> str:
    return digest_bytes(canonical_json_bytes(value))


@dataclass(frozen=True)
class WorkbenchConfig:
    schema_version: int = CURRENT_SCHEMA_VERSION
    time_unit: str = "ms"
    max_gap_ms: int = 1000
    confidence_decimals: int = 4
    srt_metadata_envelope: str = METADATA_PREFIX
    vtt_metadata_envelope: str = METADATA_PREFIX
    merge_strategies: tuple[str, ...] = ("last-wins", "mark-conflict")

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["merge_strategies"] = list(self.merge_strategies)
        return value

    def fingerprint(self) -> str:
        return digest_value(
            {
                "config": self.to_dict(),
                "codec_contract": "srt-vtt-json-v2",
            }
        )


DEFAULT_CONFIG = WorkbenchConfig()
