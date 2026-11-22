from __future__ import annotations

import datetime as dt
import os
import shutil
from pathlib import Path
from typing import Any

from .canonical import (
    atomic_write_bytes,
    atomic_write_json,
    atomic_write_jsonl,
    canonical_bytes,
    digest_obj,
    read_json,
    read_jsonl,
)
from .errors import EngineError
from .locks import AdvisoryFileLock


class Workspace:
    def __init__(self, root: Path | str) -> None:
        self.root = Path(root).resolve()
        self.config_dir = self.root / "config"
        self.input_dir = self.root / "inputs"
        self.images_dir = self.input_dir / "images"
        self.layout_path = self.config_dir / "layout.json"
        self.images_jsonl = self.input_dir / "images.jsonl"
        self.texts_jsonl = self.input_dir / "texts.jsonl"
        self.runtime_dir = self.root / "runtime"
        self.records_dir = self.runtime_dir / "records"
        self.ownership_dir = self.runtime_dir / "ownership"
        self.locks_dir = self.runtime_dir / "locks"
        self.diagnostics_dir = self.runtime_dir / "diagnostics"
        self.lock_path = self.locks_dir / "workspace.lock"
        self.index_path = self.ownership_dir / "index.json"
        self.state_path = self.ownership_dir / "state.json"
        self.vectors_path = self.ownership_dir / "vectors.json"
        self.invalidations_path = self.ownership_dir / "invalidations.json"
        self.layout_events_path = self.ownership_dir / "layout-events.json"
        self.stop_path = self.runtime_dir / "stop.json"

    def ensure(self) -> "Workspace":
        self.root.mkdir(parents=True, exist_ok=True)
        for directory in (
            self.config_dir,
            self.input_dir,
            self.images_dir,
            self.records_dir,
            self.ownership_dir,
            self.locks_dir,
            self.diagnostics_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        return self

    def lock(self) -> AdvisoryFileLock:
        self.ensure()
        return AdvisoryFileLock(self.lock_path)

    def doctor(self) -> dict[str, Any]:
        self.ensure()
        probe = self.ownership_dir / ".atomic-probe.json"
        payload = {"中文": "原子写入", "value": 1}
        atomic_write_json(probe, payload)
        loaded = read_json(probe)
        probe.unlink(missing_ok=True)
        atomic_ok = loaded == payload
        lock_ok = False
        with self.lock() as acquired:
            lock_ok = Path(acquired.path).exists()
        utf8_ok = canonical_bytes({"test": "中文/English/emoji-\U0001f680"}).decode("utf-8").encode("utf-8")
        paths = {
            "root": len(str(self.root)),
            "layout": len(str(self.layout_path)),
            "records": len(str(self.records_dir)),
        }
        return {
            "ok": bool(atomic_ok and lock_ok),
            "workspace": str(self.root),
            "python": os.sys.version.split()[0],
            "platform": os.name,
            "atomic_replace": atomic_ok,
            "file_lock": lock_ok,
            "lock_implementation": "msvcrt" if os.name == "nt" else "fcntl.flock",
            "utf8_roundtrip": bool(utf8_ok),
            "path_lengths": paths,
            "directories": {
                "records": str(self.records_dir),
                "ownership": str(self.ownership_dir),
                "locks": str(self.locks_dir),
            },
        }

    def load_layout(self) -> dict[str, Any]:
        if not self.layout_path.exists():
            raise EngineError(
                "E_INVALID_LAYOUT",
                "workspace has no layout configuration",
                "layout_validation",
                details={"path": str(self.layout_path)},
            )
        try:
            return read_json(self.layout_path)
        except (OSError, ValueError) as exc:
            raise EngineError(
                "E_INVALID_LAYOUT",
                "layout configuration is not readable JSON",
                "layout_validation",
                details={"reason": str(exc)},
            ) from exc

    def save_layout(self, normalized_layout: dict[str, Any]) -> None:
        atomic_write_json(self.layout_path, normalized_layout)

    def load_images(self) -> list[dict[str, Any]]:
        return read_jsonl(self.images_jsonl) if self.images_jsonl.exists() else []

    def load_texts(self) -> list[dict[str, Any]]:
        return read_jsonl(self.texts_jsonl) if self.texts_jsonl.exists() else []

    def normalize_image_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        seen: set[str] = set()
        for idx, row in enumerate(rows):
            image_id = row.get("image_id")
            file_name = row.get("file") or row.get("png_file") or row.get("filename") or row.get("path")
            language = row.get("language", "und")
            if not isinstance(image_id, str) or not image_id:
                raise EngineError(
                    "E_INVALID_INPUT",
                    "image_id must be a non-empty string",
                    "input_import",
                    entries=[{"line": idx + 1}],
                )
            if image_id in seen:
                raise EngineError(
                    "E_INVALID_INPUT",
                    "duplicate image_id in image catalog",
                    "input_import",
                    entries=[{"image_id": image_id}],
                )
            if not isinstance(file_name, str) or not file_name:
                raise EngineError(
                    "E_INVALID_INPUT",
                    "image file must be a non-empty string",
                    "input_import",
                    entries=[{"image_id": image_id}],
                )
            if not isinstance(language, str):
                raise EngineError(
                    "E_INVALID_INPUT",
                    "language must be a string",
                    "input_import",
                    entries=[{"image_id": image_id}],
                )
            seen.add(image_id)
            safe_name = Path(file_name).name
            output.append(
                {
                    "image_id": image_id,
                    "file": safe_name,
                    "language": language,
                    "catalog_ordinal": idx,
                }
            )
        return output

    def normalize_text_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        output: list[dict[str, Any]] = []
        seen: set[str] = set()
        for idx, row in enumerate(rows):
            text_id = row.get("text_id")
            text = row.get("text")
            image_id = row.get("image_id") or row.get("associated_image_id")
            target_shard = row.get("target_shard") or row.get("target_shard_id")
            if not isinstance(text_id, str) or not text_id:
                raise EngineError(
                    "E_INVALID_INPUT",
                    "text_id must be a non-empty string",
                    "input_import",
                    entries=[{"line": idx + 1}],
                )
            if text_id in seen:
                raise EngineError(
                    "E_INVALID_INPUT",
                    "duplicate text_id in text catalog",
                    "input_import",
                    entries=[{"text_id": text_id}],
                )
            if not isinstance(text, str):
                raise EngineError(
                    "E_INVALID_INPUT",
                    "text must be a string",
                    "input_import",
                    entries=[{"text_id": text_id}],
                )
            if not isinstance(image_id, str) or not image_id:
                raise EngineError(
                    "E_INVALID_INPUT",
                    "image_id must be a non-empty string",
                    "input_import",
                    entries=[{"text_id": text_id}],
                )
            if not isinstance(target_shard, str) or not target_shard:
                raise EngineError(
                    "E_INVALID_INPUT",
                    "target_shard must be a non-empty string",
                    "input_import",
                    entries=[{"text_id": text_id, "image_id": image_id}],
                )
            seen.add(text_id)
            output.append(
                {
                    "text_id": text_id,
                    "text": text,
                    "image_id": image_id,
                    "target_shard": target_shard,
                    "catalog_ordinal": idx,
                }
            )
        return output

    def stage_image_files(self, rows: list[dict[str, Any]], source_dir: Path | None) -> list[str]:
        copied: list[str] = []
        for row in rows:
            file_name = row["file"]
            destination = self.images_dir / file_name
            source: Path | None = None
            if source_dir is not None:
                candidate = source_dir / file_name
                if candidate.is_file():
                    source = candidate
            if source is not None and source.resolve() != destination.resolve():
                data = source.read_bytes()
                atomic_write_bytes(destination, data)
                copied.append(file_name)
            elif source is not None:
                copied.append(file_name)
        return copied

    def save_catalog(self, images: list[dict[str, Any]], texts: list[dict[str, Any]]) -> None:
        atomic_write_jsonl(self.images_jsonl, images)
        atomic_write_jsonl(self.texts_jsonl, texts)

    def resolve_image_path(self, file_name: str) -> Path:
        return self.images_dir / Path(file_name).name

    def image_bytes(self, file_name: str) -> bytes | None:
        path = self.resolve_image_path(file_name)
        try:
            return path.read_bytes()
        except OSError:
            return None

    def record_diagnostic(self, error: EngineError) -> str:
        self.ensure()
        payload = error.to_dict()
        diagnostic_id = digest_obj({"schema": "mmb-diagnostic-v1", "error": payload})
        stored = {"diagnostic_id": diagnostic_id, "error": payload}
        atomic_write_json(self.diagnostics_dir / f"error-{diagnostic_id}.json", stored)
        return diagnostic_id

    def diagnostics(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if not self.diagnostics_dir.exists():
            return rows
        for path in sorted(self.diagnostics_dir.glob("error-*.json")):
            try:
                rows.append(read_json(path))
            except (OSError, ValueError):
                continue
        return rows

    def request_stop(self, reason: str = "operator request") -> dict[str, Any]:
        self.ensure()
        payload = {
            "schema_version": "stop-request-v1",
            "reason": reason,
            "requested_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        atomic_write_json(self.stop_path, payload)
        return payload

    def clear_stop(self) -> bool:
        existed = self.stop_path.exists()
        self.stop_path.unlink(missing_ok=True)
        return existed

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def read_stop(self) -> dict[str, Any] | None:
        if not self.stop_path.exists():
            return None
        try:
            return read_json(self.stop_path)
        except (OSError, ValueError):
            return {"schema_version": "stop-request-v1", "reason": "unreadable stop marker"}

    def copy_workspace_files(self, destination: Path) -> None:
        destination.mkdir(parents=True, exist_ok=True)
        for name in ("config", "inputs"):
            src = self.root / name
            if src.exists():
                target = destination / name
                if target.exists():
                    shutil.rmtree(target)
                shutil.copytree(src, target)
