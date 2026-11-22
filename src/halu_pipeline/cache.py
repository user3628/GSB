from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .canonical import (
    atomic_write_json,
    canonical_json,
    exclusive_file_lock,
    read_json,
    sha256_obj,
)


class QueryCache:
    """Persistent byte cache keyed by content/config/stage fingerprints."""

    def __init__(self, run_dir: Path | str, max_entries: int = 256) -> None:
        self.run_dir = Path(run_dir)
        self.path = self.run_dir / "cache" / "query_cache.json"
        self.lock_path = self.run_dir / "cache" / ".query_cache.lock"
        self.max_entries = max(1, int(max_entries))
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def make_key(
        self,
        *,
        kind: str,
        content_fingerprint: str,
        config_version: str,
        stage_fingerprint: str,
        scope: str = "default",
    ) -> str:
        return sha256_obj(
            {
                "kind": kind,
                "content_fingerprint": content_fingerprint,
                "config_version": config_version,
                "stage_fingerprint": stage_fingerprint,
                "scope": scope,
            }
        )

    def _load(self) -> dict[str, str]:
        payload = read_json(self.path, default={"entries": {}})
        if not isinstance(payload, dict) or not isinstance(payload.get("entries"), dict):
            return {}
        return {
            str(key): str(value)
            for key, value in payload["entries"].items()
            if isinstance(key, str) and isinstance(value, str)
        }

    def get(self, key: str) -> bytes | None:
        value = self._load().get(key)
        if value is None:
            return None
        return value.encode("utf-8")

    def put(self, key: str, data: bytes) -> None:
        text = data.decode("utf-8")
        # Parsing here rejects accidental non-JSON cache values before publishing.
        canonical_json(json.loads(text))
        with exclusive_file_lock(self.lock_path, blocking=True):
            entries = self._load()
            entries[key] = text
            if len(entries) > self.max_entries:
                for stale_key in sorted(entries)[: len(entries) - self.max_entries]:
                    entries.pop(stale_key, None)
            atomic_write_json(self.path, {"entries": entries})

    def clear(self) -> None:
        with exclusive_file_lock(self.lock_path, blocking=True):
            atomic_write_json(self.path, {"entries": {}})

    def stats(self) -> dict[str, Any]:
        entries = self._load()
        return {"entry_count": len(entries), "keys": sorted(entries)}
