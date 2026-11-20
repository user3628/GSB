"""Content-addressed artifact cache used for incremental stage reuse."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from .canonical import atomic_write_json, digest, read_json


class ArtifactCache:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def path_for(self, kind: str, fingerprint: str) -> Path:
        return self.root / kind / f"{fingerprint}.json"

    def get(self, kind: str, fingerprint: str) -> tuple[dict[str, Any] | None, str | None]:
        path = self.path_for(kind, fingerprint)
        if not path.exists():
            return None, None
        try:
            envelope = read_json(path)
        except (OSError, ValueError):
            return None, None
        if (
            not isinstance(envelope, dict)
            or envelope.get("kind") != kind
            or envelope.get("fingerprint") != fingerprint
            or not isinstance(envelope.get("artifact"), dict)
            or envelope.get("checksum") != digest(envelope["artifact"])
        ):
            return None, None
        return envelope["artifact"], str(path)

    def put(self, kind: str, fingerprint: str, artifact: dict[str, Any]) -> str:
        path = self.path_for(kind, fingerprint)
        envelope = {
            "schema_version": 1,
            "kind": kind,
            "fingerprint": fingerprint,
            "checksum": digest(artifact),
            "artifact": artifact,
        }
        atomic_write_json(path, envelope)
        return str(path)
