from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from app.manifest import canonical_object_digest


ROOT = Path(__file__).resolve().parent.parent
SESSIONS_PATH = ROOT / "samples" / "sessions.json"


def deterministic_part_bytes(session_id: str, seq: int, size_bytes: int) -> bytes:
    seed = hashlib.sha256(f"{session_id}:{seq}".encode("utf-8")).digest()
    repeats = (size_bytes + len(seed) - 1) // len(seed)
    return (seed * repeats)[:size_bytes]


def load_samples() -> dict[str, Any]:
    with SESSIONS_PATH.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def sample(name: str) -> dict[str, Any]:
    for item in load_samples()["sessions"]:
        if item["session_id"] == name:
            return item
    raise KeyError(name)


def verify_part_hashes() -> list[dict[str, Any]]:
    failures = []
    for session in load_samples()["sessions"]:
        for part in session["parts"]:
            actual = hashlib.sha256(
                deterministic_part_bytes(session["session_id"], part["seq"], part["size_bytes"])
            ).hexdigest()
            if actual != part["sha256"]:
                failures.append(
                    {
                        "session_id": session["session_id"],
                        "seq": part["seq"],
                        "expected": part["sha256"],
                        "actual": actual,
                    }
                )
    return failures


def expected_object_digest(session: dict[str, Any]) -> str:
    return canonical_object_digest(session["parts"])
