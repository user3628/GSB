from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable


def canonical_object_digest(parts: Iterable[dict[str, Any]]) -> str:
    """Hash a canonical, sequence-ordered part manifest.

    The digest does not depend on network arrival order or SQLite row order. It is
    deliberately a manifest digest because the gateway contracts carry part sizes
    and SHA-256 values, not the raw object bytes.
    """

    digest = hashlib.sha256()
    for part in sorted(parts, key=lambda item: int(item["seq"])):
        canonical = json.dumps(
            {
                "seq": int(part["seq"]),
                "size_bytes": int(part["size_bytes"]),
                "sha256": str(part["sha256"]).lower(),
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("ascii")
        digest.update(canonical)
        digest.update(b"\n")
    return digest.hexdigest()


def manifest_from_parts(parts: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "seq": int(part["seq"]),
            "size_bytes": int(part["size_bytes"]),
            "sha256": str(part["sha256"]).lower(),
            "arrived_at": part["arrived_at"],
            "verified_at": part.get("verified_at"),
        }
        for part in sorted(parts, key=lambda item: int(item["seq"]))
    ]


def is_complete(parts: Iterable[dict[str, Any]], total_parts: int) -> bool:
    verified = {
        int(part["seq"])
        for part in parts
        if part.get("status") == "VERIFIED"
    }
    return verified == set(range(1, total_parts + 1))
