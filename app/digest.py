from __future__ import annotations

import hashlib
import json
from typing import Any

from app.models import PolicySnapshot, TraceSample


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def sha256_text(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def policy_checksum(snapshot: PolicySnapshot) -> str:
    payload = snapshot.model_dump(mode="json", exclude={"checksum"})
    return sha256_text(canonical_json(payload))


def trace_checksum(trace: TraceSample) -> str:
    return sha256_text(canonical_json(trace.model_dump(mode="json")))
