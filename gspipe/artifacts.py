"""Content-addressed JSON/PNG artifacts with integrity checks."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import canonical_json_bytes, sha256_bytes
from .errors import E_ARTIFACT_CORRUPT, E_IO, PipelineError
from .fsutil import atomic_write_bytes, safe_relative_path


@dataclass(frozen=True)
class ArtifactRef:
    digest: str
    kind: str
    relative_path: str

    def to_dict(self) -> dict[str, str]:
        return {
            "artifact_digest": self.digest,
            "artifact_type": self.kind,
            "artifact_path": self.relative_path,
        }


class ArtifactStore:
    def __init__(self, workspace_root: str | Path) -> None:
        self.workspace_root = Path(workspace_root)
        self.root = self.workspace_root / ".gspipe" / "artifacts"
        self.json_root = self.root / "json"
        self.png_root = self.root / "png"

    def _path_for(self, digest: str, kind: str) -> Path:
        extension = "json" if kind == "json" else "png"
        base = self.json_root if kind == "json" else self.png_root
        return base / digest[:2] / f"{digest}.{extension}"

    def _relative(self, path: Path) -> str:
        return path.relative_to(self.workspace_root).as_posix()

    def _put(self, data: bytes, kind: str) -> ArtifactRef:
        digest = sha256_bytes(data)
        target = self._path_for(digest, kind)
        if not target.exists():
            atomic_write_bytes(target, data)
        else:
            try:
                existing = target.read_bytes()
            except OSError:
                existing = b""
            if sha256_bytes(existing) != digest:
                atomic_write_bytes(target, data)
        return ArtifactRef(digest=digest, kind=kind, relative_path=self._relative(target))

    def put_json(self, value: Any) -> ArtifactRef:
        return self._put(canonical_json_bytes(value), "json")

    def put_png(self, png_bytes: bytes) -> ArtifactRef:
        return self._put(png_bytes, "png")

    def _get(self, digest: str, kind: str) -> bytes:
        target = self._path_for(digest, kind)
        try:
            data = target.read_bytes()
        except OSError as exc:
            raise PipelineError(
                E_ARTIFACT_CORRUPT,
                f"Missing {kind} artifact {digest}",
                details={"digest": digest, "kind": kind, "path": str(target)},
            ) from exc
        actual = sha256_bytes(data)
        if actual != digest:
            raise PipelineError(
                E_ARTIFACT_CORRUPT,
                f"Artifact digest mismatch for {digest}",
                details={"expected": digest, "actual": actual, "kind": kind},
            )
        return data

    def get_json(self, digest: str) -> Any:
        try:
            return json.loads(self._get(digest, "json").decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PipelineError(
                E_ARTIFACT_CORRUPT,
                f"Invalid JSON artifact {digest}",
                details={"digest": digest},
            ) from exc

    def get_png(self, digest: str) -> bytes:
        return self._get(digest, "png")

    def exists(self, digest: str, kind: str) -> bool:
        try:
            self._get(digest, kind)
            return True
        except PipelineError:
            return False

    def read_ref(self, kind: str, relative_path: str) -> bytes:
        target = safe_relative_path(self.workspace_root, relative_path)
        try:
            data = target.read_bytes()
        except OSError as exc:
            raise PipelineError(
                E_IO,
                f"Unable to read artifact {relative_path}: {exc}",
                details={"path": relative_path},
            ) from exc
        return data
