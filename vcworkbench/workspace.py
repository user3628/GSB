from __future__ import annotations

import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import FileLock, atomic_write_json, read_json
from .errors import fail
from .manifest import Manifest, load_manifest


@dataclass
class Workspace:
    root: Path

    def __post_init__(self) -> None:
        self.root = Path(self.root).resolve()

    @property
    def manifest_path(self) -> Path:
        return self.root / "manifest.json"

    @property
    def samples_dir(self) -> Path:
        return self.root / "samples"

    @property
    def state_dir(self) -> Path:
        return self.root / "state"

    @property
    def exports_dir(self) -> Path:
        return self.root / "exports"

    @property
    def control_dir(self) -> Path:
        return self.state_dir / "control"

    @property
    def stop_path(self) -> Path:
        return self.control_dir / "stop.json"

    def state_lock(self) -> FileLock:
        return FileLock(self.state_dir / "workspace.lock")

    def ensure(self) -> None:
        for path in (
            self.root,
            self.samples_dir,
            self.state_dir,
            self.state_dir / "artifacts",
            self.state_dir / "shards",
            self.state_dir / "control",
            self.exports_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def has_manifest(self) -> bool:
        return self.manifest_path.is_file()

    def require_manifest(self) -> Manifest:
        if not self.has_manifest():
            fail(
                "E_WORKSPACE_NOT_READY",
                "workspace has no manifest; run `python -m vcworkbench demo` first",
                stage="ingest",
                workspace=str(self.root),
            )
        return load_manifest(self.manifest_path)

    def write_manifest(self, data: dict[str, Any], *, lock: bool = True) -> Manifest:
        self.ensure()

        def _write() -> Manifest:
            atomic_write_json(self.manifest_path, data, pretty=True)
            return load_manifest(self.manifest_path)

        if lock:
            with self.state_lock():
                return _write()
        return _write()

    def import_manifest(self, external_manifest: Path) -> Manifest:
        external_manifest = Path(external_manifest).resolve()
        incoming = load_manifest(external_manifest)
        self.ensure()
        with self.state_lock():
            existing: Manifest | None = None
            if self.manifest_path.is_file():
                existing = load_manifest(self.manifest_path)
            by_id: dict[str, dict[str, Any]] = {}
            if existing is not None:
                for spec in existing.shards:
                    by_id[spec.shard_id] = dict(spec.raw)
            external_root = external_manifest.parent
            for spec in incoming.shards:
                source_wav = spec.resolve_wav(external_root)
                if not source_wav.is_file():
                    fail(
                        "E_MANIFEST_MISMATCH",
                        "imported shard WAV is missing",
                        shard_id=spec.shard_id,
                        stage="ingest",
                        path=str(source_wav),
                    )
                suffix = source_wav.suffix.lower() or ".wav"
                dest_name = f"import_{_safe_name(spec.shard_id)}{suffix}"
                dest = self.samples_dir / dest_name
                shutil.copyfile(source_wav, dest)
                raw = dict(spec.raw)
                raw["wav"] = f"samples/{dest_name}"
                by_id[spec.shard_id] = raw
            merged = {
                "version": incoming.version,
                "shards": [by_id[key] for key in sorted(by_id)],
            }
            atomic_write_json(self.manifest_path, merged, pretty=True)
            return load_manifest(self.manifest_path)

    def stop_status(self) -> dict[str, Any]:
        if not self.stop_path.is_file():
            return {"requested": False}
        try:
            data = read_json(self.stop_path)
        except Exception:
            return {"requested": True, "reason": "unreadable stop marker"}
        data.setdefault("requested", True)
        return data

    def request_stop(self, *, reason: str = "operator request", scope: list[str] | None = None) -> dict[str, Any]:
        self.ensure()
        payload = {
            "requested": True,
            "reason": reason,
            "scope": scope or ["all"],
        }
        with self.state_lock():
            atomic_write_json(self.stop_path, payload)
        return payload

    def clear_stop(self) -> None:
        self.ensure()
        with self.state_lock():
            if self.stop_path.exists():
                self.stop_path.unlink()

    def should_stop(self, shard_id: str | None = None) -> bool:
        status = self.stop_status()
        if not status.get("requested"):
            return False
        scope = status.get("scope", ["all"])
        return "all" in scope or (shard_id is not None and shard_id in scope)


def _safe_name(value: str) -> str:
    import hashlib

    readable = "".join(c if c.isalnum() or c in "._-" else "_" for c in value)[:36]
    return f"{readable}_{hashlib.sha256(value.encode('utf-8')).hexdigest()[:8]}"
