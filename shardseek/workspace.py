"""Workspace bootstrap, manifest import, active config, and control files."""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Any

from .checkpoint import CheckpointStore
from .config import (
    config_fingerprint,
    feature_fingerprint,
    index_fingerprint,
    load_config,
    projection_quantization_fingerprint,
    quantization_fingerprint,
    validate_config,
)
from .errors import E_EMPTY_CAPTION, E_INVALID_MANIFEST, error
from .features import normalize_caption
from .util import (
    canonical_bytes,
    digest_obj,
    read_json,
    sha256_bytes,
    sha256_file,
    write_atomic,
    write_json_atomic,
)


ITEM_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
REQUIRED_MANIFEST_FIELDS = ("item_id", "image_file", "caption", "lang", "source")


class Workspace:
    def __init__(self, root: str | Path = "runtime", *, project_root: str | Path | None = None) -> None:
        self.root = Path(root).resolve()
        self.project_root = Path(project_root or Path.cwd()).resolve()
        self.config_dir = self.root / "config" / "active.json"
        self.import_dir = self.root / "import"
        self.import_images_dir = self.import_dir / "images"
        self.import_items_path = self.import_dir / "items.json"
        self.state_dir = self.root / "state"
        self.library_dir = self.root / "library"
        self.diagnostics_dir = self.root / "diagnostics"
        self.reports_dir = self.root / "reports"
        self.control_path = self.state_dir / "control.json"
        self.locks_dir = self.state_dir / "locks"
        self.publish_lock_path = self.locks_dir / "publish.lock"
        self.snapshot_state_path = self.state_dir / "current_snapshot.json"
        self.build_state_path = self.state_dir / "build_state.json"
        self.features_path = self.state_dir / "features.json"
        self.representation_path = self.library_dir / "representations.json"
        self.reference_path = self.diagnostics_dir / "reference_vectors.json"
        self.index_path = self.library_dir / "index.json"
        self.representation_summary_path = self.library_dir / "representation_summary.json"
        self.index_summary_path = self.library_dir / "index_summary.json"
        self.config_snapshot_path = self.library_dir / "config_snapshot.json"

    def init(self) -> "Workspace":
        for path in (
            self.root,
            self.config_dir.parent,
            self.import_dir,
            self.import_images_dir,
            self.state_dir,
            self.library_dir,
            self.diagnostics_dir,
            self.reports_dir,
            self.locks_dir,
            self.state_dir / "checkpoints_archive",
        ):
            path.mkdir(parents=True, exist_ok=True)
        if not self.control_path.exists():
            write_json_atomic(self.control_path, {"stop_requested": False})
        if not self.config_dir.exists():
            default = self.project_root / "configs" / "baseline_8bit.json"
            if default.exists():
                config = load_config(default, self.project_root)
                write_json_atomic(self.config_dir, config, pretty=True)
        return self

    def clean(self) -> None:
        if self.root == Path("/"):
            raise error(E_INVALID_MANIFEST, "Refusing to clean filesystem root", stage="workspace")
        if self.root.exists():
            shutil.rmtree(self.root)
        self.init()

    def set_config(self, config_name_or_path: str | Path | dict[str, Any]) -> dict[str, Any]:
        if isinstance(config_name_or_path, dict):
            config = config_name_or_path
            validate_config(config)
        else:
            config = load_config(config_name_or_path, self.project_root)
        self.init()
        write_json_atomic(self.config_dir, config, pretty=True)
        return config

    def active_config(self) -> dict[str, Any]:
        self.init()
        if not self.config_dir.exists():
            raise error(E_INVALID_MANIFEST, "No active configuration has been selected", stage="config")
        config = read_json(self.config_dir)
        validate_config(config)
        return config

    def import_manifest(self, manifest_path: str | Path) -> dict[str, Any]:
        self.init()
        source = Path(manifest_path)
        if not source.is_file():
            raise error(
                E_INVALID_MANIFEST,
                "Manifest file not found",
                stage="import",
                details={"manifest": str(manifest_path)},
            )
        records: list[dict[str, Any]] = []
        seen: set[str] = set()
        try:
            with open(source, "rb") as handle:
                lines = handle.read().decode("utf-8").splitlines()
        except UnicodeDecodeError as exc:
            raise error(
                E_INVALID_MANIFEST,
                "Manifest must be UTF-8 JSONL",
                stage="import",
                details={"reason": type(exc).__name__},
            ) from exc
        for line_number, line in enumerate(lines, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise error(
                    E_INVALID_MANIFEST,
                    "Manifest line is not valid JSON",
                    stage="import",
                    details={"line": line_number, "reason": str(exc)},
                ) from exc
            if not isinstance(record, dict):
                raise error(
                    E_INVALID_MANIFEST,
                    "Manifest line must be a JSON object",
                    stage="import",
                    details={"line": line_number},
                )
            missing = [field for field in REQUIRED_MANIFEST_FIELDS if field not in record]
            if missing:
                raise error(
                    E_INVALID_MANIFEST,
                    "Manifest record is missing required fields",
                    stage="import",
                    item_id=str(record.get("item_id", "")) or None,
                    details={"line": line_number, "missing": missing},
                )
            item_id = record["item_id"]
            if not isinstance(item_id, str) or not ITEM_ID_RE.match(item_id):
                raise error(
                    E_INVALID_MANIFEST,
                    "item_id contains unsupported characters",
                    stage="import",
                    item_id=str(item_id),
                    details={"line": line_number},
                )
            if item_id in seen:
                raise error(
                    E_INVALID_MANIFEST,
                    "Duplicate item_id",
                    stage="import",
                    item_id=item_id,
                    details={"line": line_number},
                )
            seen.add(item_id)
            enabled = bool(record.get("enabled", True))
            expected_error = record.get("expected_error")
            image_path = (source.parent / str(record["image_file"])).resolve()
            if not image_path.is_file():
                raise error(
                    E_INVALID_MANIFEST,
                    "Referenced image file does not exist",
                    stage="import",
                    item_id=item_id,
                    details={"line": line_number, "image_file": str(record["image_file"])},
                )
            error_record: dict[str, Any] | None = None
            if enabled:
                try:
                    normalize_caption(str(record["caption"]), item_id=item_id)
                except Exception as exc:
                    if hasattr(exc, "to_dict"):
                        raise
                    raise
            else:
                # Quarantined anomalies are surfaced explicitly, never silently skipped.
                try:
                    normalize_caption(str(record["caption"]), item_id=item_id)
                except Exception as exc:
                    to_dict = getattr(exc, "to_dict", None)
                    if callable(to_dict):
                        structured = to_dict()["error"]
                        error_record = {
                            "item_id": item_id,
                            "code": structured["code"],
                            "stage": structured["stage"],
                            "message": structured["message"],
                        }
                    else:
                        raise
            destination = self.import_images_dir / f"{item_id}.png"
            payload = image_path.read_bytes()
            write_atomic(destination, payload)
            item = {
                "item_id": item_id,
                "caption": str(record["caption"]),
                "lang": str(record["lang"]),
                "source": str(record["source"]),
                "enabled": enabled,
                "image_file": str(destination.relative_to(self.root)),
                "image_sha256": sha256_bytes(payload),
                "manifest_line": line_number,
            }
            if expected_error is not None:
                item["expected_error"] = str(expected_error)
            if error_record is not None:
                item["import_error"] = error_record
            records.append(item)

        records.sort(key=lambda item: item["item_id"])
        enabled_items = [item for item in records if item["enabled"]]
        if not enabled_items:
            raise error(
                E_INVALID_MANIFEST,
                "Manifest contains no enabled items",
                stage="import",
                details={"records": len(records)},
            )
        rejected = [item["import_error"] for item in records if "import_error" in item]
        manifest_digest = digest_obj(records)
        payload = {
            "format": "shardseek-import-v1",
            "manifest_name": source.name,
            "manifest_sha256": manifest_digest,
            "item_count": len(records),
            "enabled_count": len(enabled_items),
            "rejected": rejected,
            "items": records,
        }
        write_json_atomic(self.import_items_path, payload, pretty=True)
        return payload

    def load_import(self) -> dict[str, Any]:
        self.init()
        if not self.import_items_path.exists():
            raise error(
                E_INVALID_MANIFEST,
                "No manifest has been imported",
                stage="import",
                details={"expected": str(self.import_items_path.relative_to(self.root))},
            )
        return read_json(self.import_items_path)

    def enabled_items(self) -> list[dict[str, Any]]:
        payload = self.load_import()
        items = [item for item in payload["items"] if item["enabled"]]
        return sorted(items, key=lambda item: item["item_id"])

    def image_path(self, item_id: str) -> Path:
        for item in self.load_import()["items"]:
            if item["item_id"] == item_id:
                return self.root / item["image_file"]
        raise error(E_INVALID_MANIFEST, "Unknown item_id", stage="import", item_id=item_id)

    def request_stop(self) -> dict[str, Any]:
        self.init()
        state = {"stop_requested": True}
        write_json_atomic(self.control_path, state)
        return state

    def clear_stop(self) -> dict[str, Any]:
        self.init()
        state = {"stop_requested": False}
        write_json_atomic(self.control_path, state)
        return state

    def stop_requested(self) -> bool:
        if not self.control_path.exists():
            return False
        try:
            return bool(read_json(self.control_path).get("stop_requested", False))
        except Exception:
            return False

    def checkpoint_store(self) -> CheckpointStore:
        self.init()
        return CheckpointStore(self.state_dir)

    def status(self) -> dict[str, Any]:
        self.init()
        config = self.active_config()
        imported: dict[str, Any] | None = None
        if self.import_items_path.exists():
            imported = read_json(self.import_items_path)
        snapshot = read_json(self.snapshot_state_path) if self.snapshot_state_path.exists() else None
        checkpoint_status = self.checkpoint_store().status()
        library_ready = all(
            path.exists()
            for path in (self.representation_path, self.index_path, self.snapshot_state_path)
        )
        return {
            "ok": True,
            "workspace": {
                "root": str(self.root),
                "writable": self.root.exists() and self.root.is_dir(),
                "file_locking": "fcntl.flock",
            },
            "python": {
                "version": __import__("platform").python_version(),
                "numpy": __import__("numpy").__version__,
                "pillow": __import__("PIL").__version__,
            },
            "config": {
                "name": config.get("name"),
                "fingerprint": config_fingerprint(config),
                "feature_fingerprint": feature_fingerprint(config),
                "projection_quantization_fingerprint": projection_quantization_fingerprint(config),
                "quantization_fingerprint": quantization_fingerprint(config),
                "index_fingerprint": index_fingerprint(config),
                "bits": config["quantization"]["bits"],
                "granularity": config["quantization"]["granularity"],
            },
            "import": {
                "manifest_sha256": imported.get("manifest_sha256") if imported else None,
                "item_count": imported.get("item_count", 0) if imported else 0,
                "enabled_count": imported.get("enabled_count", 0) if imported else 0,
                "rejected": imported.get("rejected", []) if imported else [],
                "items": [
                    {
                        "item_id": item["item_id"],
                        "source": item["source"],
                        "lang": item["lang"],
                        "caption": item["caption"],
                        "enabled": item["enabled"],
                    }
                    for item in (imported.get("items", []) if imported else [])
                ],
            },
            "library": {
                "ready": library_ready,
                "snapshot": snapshot,
            },
            "checkpoints": checkpoint_status,
            "stop_requested": self.stop_requested(),
        }

    def fingerprints(self) -> dict[str, Any]:
        self.init()
        config = self.active_config()
        build_state = read_json(self.build_state_path) if self.build_state_path.exists() else None
        snapshot = read_json(self.snapshot_state_path) if self.snapshot_state_path.exists() else None
        return {
            "ok": True,
            "active_config": {
                "name": config.get("name"),
                "config": config_fingerprint(config),
                "feature": feature_fingerprint(config),
                "projection_quantization": projection_quantization_fingerprint(config),
                "quantization": quantization_fingerprint(config),
                "index": index_fingerprint(config),
            },
            "published_build": build_state,
            "snapshot": snapshot,
            "checkpoints": self.checkpoint_store().status(),
        }

    def canonical_state_digest(self) -> str:
        return digest_obj(self.status())

