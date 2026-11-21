"""Query cache keyed by shard, config version, and angle-table fingerprints."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from .digest import (
    atomic_write_bytes,
    canonical_json_bytes,
    digest_json,
    read_json,
    sha256_bytes,
)


class QueryCache:
    def __init__(self, workspace: str | Path) -> None:
        self.workspace = Path(workspace).resolve()
        self.root = self.workspace / "cache" / "queries"

    def key_components(
        self,
        *,
        shard_fingerprint: str,
        config_version: str,
        angle_table_fingerprint: str,
        query_type: str,
        options: Mapping[str, Any],
    ) -> dict[str, str]:
        return {
            "shard_fingerprint": shard_fingerprint,
            "config_version": config_version,
            "angle_table_fingerprint": angle_table_fingerprint,
            "query_type": query_type,
            "options_digest": digest_json(dict(options)),
        }

    def key(self, components: Mapping[str, str]) -> str:
        return sha256_bytes(canonical_json_bytes({"v": 1, **components}))

    def _payload_path(self, key: str) -> Path:
        return self.root / f"{key}.json"

    def _digest_path(self, key: str) -> Path:
        return self.root / f"{key}.sha256"

    def get(self, key: str) -> bytes | None:
        payload_path = self._payload_path(key)
        digest_path = self._digest_path(key)
        if not payload_path.exists() or not digest_path.exists():
            return None
        payload = payload_path.read_bytes()
        stored_digest = digest_path.read_text(encoding="ascii").strip()
        if sha256_bytes(payload) != stored_digest:
            return None
        return payload

    def put(self, key: str, payload: bytes) -> None:
        atomic_write_bytes(self._payload_path(key), payload)
        atomic_write_bytes(
            self._digest_path(key), sha256_bytes(payload).encode("ascii") + b"\n"
        )
