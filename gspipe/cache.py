"""Execution-result and deterministic query caches."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .artifacts import ArtifactStore
from .canonical import canonical_json_bytes, sha256_json
from .fsutil import atomic_write_bytes


@dataclass(frozen=True)
class ExecutionCacheHit:
    key: str
    metadata: dict[str, Any]
    png_bytes: bytes


class ExecutionCache:
    def __init__(self, workspace_root: str | Path, artifacts: ArtifactStore) -> None:
        self.root = Path(workspace_root) / ".gspipe" / "cache" / "execution"
        self.artifacts = artifacts

    def key_for(self, *, input_pixels: str, chain: list[dict[str, Any]], kernel_version: str) -> str:
        return sha256_json(
            {
                "schema": "gspipe-execution-cache-key-v1",
                "input_pixels": input_pixels,
                "chain": chain,
                "kernel_version": kernel_version,
            }
        )

    def _path(self, key: str) -> Path:
        return self.root / key[:2] / f"{key}.json"

    def get(self, key: str) -> ExecutionCacheHit | None:
        path = self._path(key)
        try:
            metadata = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        if not isinstance(metadata, dict) or metadata.get("key") != key:
            return None
        output_digest = metadata.get("output_png_digest")
        if not isinstance(output_digest, str):
            return None
        try:
            png_bytes = self.artifacts.get_png(output_digest)
        except Exception:
            return None
        return ExecutionCacheHit(key=key, metadata=metadata, png_bytes=png_bytes)

    def put(self, key: str, metadata: dict[str, Any], png_bytes: bytes) -> None:
        payload = dict(metadata)
        payload["key"] = key
        atomic_write_bytes(self._path(key), canonical_json_bytes(payload) + b"\n")


class QueryCache:
    def __init__(self, workspace_root: str | Path) -> None:
        self.root = Path(workspace_root) / ".gspipe" / "cache" / "query"

    def _path(self, key: str) -> Path:
        return self.root / key[:2] / f"{key}.json"

    def get(self, key: str) -> bytes | None:
        path = self._path(key)
        try:
            data = path.read_bytes()
        except OSError:
            return None
        try:
            parsed = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        if not isinstance(parsed, dict) or parsed.get("cache_key") != key:
            return None
        return data

    def put(self, key: str, data: bytes) -> None:
        # The stored bytes are the canonical response payload.  cache_key is
        # part of that deterministic payload, making corruption detectable
        # without altering bytes between a hit and a recomputation.
        atomic_write_bytes(self._path(key), data)
