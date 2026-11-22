"""Workspace layout, import, stop control, and run-lock helpers."""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np

from .canonical import canonical_json_bytes, sha256_json
from .config import PipelineConfig, deep_merge, load_config
from .errors import E_INVALID_INPUT, E_INVALID_TASKS, E_IO, PipelineError
from .fsutil import atomic_write_bytes, atomic_write_json, ensure_dir
from .locking import FileLock
from .models import TaskSpec, parse_task_manifest, tasks_to_manifest, validate_input_filename
from .pngio import decode_rgb_png, image_info
from .sample_data import generate_samples


@dataclass(frozen=True)
class InputShard:
    filename: str
    png_bytes: bytes
    image: np.ndarray
    info: dict[str, Any]


class Workspace:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.inputs_dir = self.root / "inputs"
        self.shards_dir = self.inputs_dir / "shards"
        self.tasks_path = self.inputs_dir / "tasks.json"
        self.config_path = self.root / "config.json"
        self.internal_dir = self.root / ".gspipe"
        self.state_dir = self.internal_dir / "state"
        self.locks_dir = self.state_dir / "locks"
        self.stop_path = self.state_dir / "stop.request"
        self.run_lock_path = self.locks_dir / "run.lock"
        self.exports_dir = self.root / "exports"

    def ensure_layout(self) -> None:
        for directory in (
            self.inputs_dir,
            self.shards_dir,
            self.state_dir,
            self.locks_dir,
            self.internal_dir / "artifacts",
            self.internal_dir / "checkpoints",
            self.internal_dir / "cache",
            self.internal_dir / "metrics",
            self.exports_dir,
        ):
            ensure_dir(directory)

    def run_lock(self, timeout: float = 15.0) -> FileLock:
        return FileLock(self.run_lock_path, timeout=timeout)

    def load_config(self) -> PipelineConfig:
        return load_config(self.config_path if self.config_path.exists() else None)

    def save_config(self, config: PipelineConfig | Mapping[str, Any]) -> None:
        raw = config.to_dict() if isinstance(config, PipelineConfig) else dict(config)
        validated = PipelineConfig(raw)
        atomic_write_json(self.config_path, validated.to_dict())

    def load_tasks(self, *, default_latency_ms: int = 2000) -> list[TaskSpec]:
        try:
            raw = json.loads(self.tasks_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineError(
                E_INVALID_TASKS,
                f"Unable to read task manifest {self.tasks_path}: {exc}",
                details={"path": str(self.tasks_path)},
            ) from exc
        return parse_task_manifest(raw, default_latency_ms=default_latency_ms)

    def decode_shard(self, filename: str, config: PipelineConfig) -> InputShard:
        validate_input_filename(filename)
        path = self.shards_dir / filename
        try:
            data = path.read_bytes()
        except OSError as exc:
            raise PipelineError(
                E_INVALID_INPUT,
                f"Missing input shard {filename}: {exc}",
                details={"filename": filename, "path": str(path)},
            ) from exc
        image = decode_rgb_png(data, source=filename)
        height, width = image.shape[:2]
        if (
            width > int(config.limits["max_image_side"])
            or height > int(config.limits["max_image_side"])
            or width * height > int(config.limits["max_image_pixels"])
        ):
            raise PipelineError(
                E_INVALID_INPUT,
                f"Input shard {filename} exceeds configured limits",
                details={
                    "filename": filename,
                    "width": width,
                    "height": height,
                    "max_image_side": config.limits["max_image_side"],
                    "max_image_pixels": config.limits["max_image_pixels"],
                },
            )
        return InputShard(filename=filename, png_bytes=data, image=image, info=image_info(image))

    def import_data(
        self,
        shards: Mapping[str, bytes] | Sequence[tuple[str, bytes]],
        tasks_manifest: Any,
        *,
        config_override: Mapping[str, Any] | None = None,
        require_all_inputs: bool = True,
    ) -> dict[str, Any]:
        self.ensure_layout()
        if isinstance(shards, Mapping):
            shard_items = list(shards.items())
        else:
            shard_items = list(shards)
        if config_override is not None and self.config_path.exists():
            try:
                existing_raw = json.loads(self.config_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                existing_raw = {}
            config = PipelineConfig(deep_merge(existing_raw, config_override))
        else:
            config = load_config(self.config_path if self.config_path.exists() else None, config_override)
        tasks = parse_task_manifest(tasks_manifest)
        if len(tasks) > int(config.limits["max_tasks"]):
            raise PipelineError(
                E_INVALID_TASKS,
                "Task count exceeds configured limit",
                details={"task_count": len(tasks), "max_tasks": config.limits["max_tasks"]},
            )

        shard_map: dict[str, bytes] = {}
        shard_info: dict[str, Any] = {}
        for name, data in shard_items:
            validate_input_filename(name)
            if not isinstance(data, (bytes, bytearray)):
                raise PipelineError(E_INVALID_INPUT, f"Shard {name} must contain PNG bytes")
            if len(data) > int(config.limits["max_import_bytes"]):
                raise PipelineError(
                    E_INVALID_INPUT,
                    f"Shard {name} exceeds import size limit",
                    details={"filename": name, "bytes": len(data)},
                )
            image = decode_rgb_png(bytes(data), source=name)
            height, width = image.shape[:2]
            if (
                width > int(config.limits["max_image_side"])
                or height > int(config.limits["max_image_side"])
                or width * height > int(config.limits["max_image_pixels"])
            ):
                raise PipelineError(
                    E_INVALID_INPUT,
                    f"Shard {name} exceeds configured limits",
                    details={"filename": name, "width": width, "height": height},
                )
            shard_map[name] = bytes(data)
            shard_info[name] = image_info(image)

        required = {task.input_filename for task in tasks}
        if require_all_inputs:
            missing = sorted(required - set(shard_map))
            if missing:
                raise PipelineError(
                    E_INVALID_TASKS,
                    "Task manifest references missing shards",
                    details={"missing": missing},
                )

        with self.run_lock():
            atomic_write_json(self.config_path, config.to_dict())
            for name, data in sorted(shard_map.items()):
                atomic_write_bytes(self.shards_dir / name, data)
            # The task manifest is the commit marker.  It is written last so a
            # crash cannot expose tasks whose shard bytes are only half copied.
            atomic_write_json(self.tasks_path, tasks_to_manifest(tasks))

        return {
            "ok": True,
            "workspace": str(self.root),
            "shards": [{"filename": name, **shard_info[name]} for name in sorted(shard_info)],
            "task_count": len(tasks),
            "task_ids": [task.task_id for task in tasks],
            "config_version": config.config_version,
            "manifest_fingerprint": sha256_json(tasks_to_manifest(tasks)),
        }

    def init_demo(self, *, force: bool = False) -> dict[str, Any]:
        if self.tasks_path.exists() and not force:
            raise PipelineError(
                E_INVALID_INPUT,
                "Workspace already contains an imported task manifest; use force to replace it",
                details={"tasks_path": str(self.tasks_path)},
            )
        shards, tasks_manifest, config = generate_samples()
        return self.import_data(shards, tasks_manifest, config_override=config)

    def request_stop(self, reason: str = "user_requested") -> dict[str, Any]:
        self.ensure_layout()
        payload = {"schema": "gspipe-stop-request-v1", "reason": reason}
        atomic_write_json(self.stop_path, payload)
        return {"ok": True, "stop_requested": True, "reason": reason}

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise PipelineError(E_IO, f"Unable to clear stop request: {exc}") from exc

    def import_http_payload(self, payload: Mapping[str, Any]) -> dict[str, Any]:
        raw_shards = payload.get("shards")
        if not isinstance(raw_shards, list):
            raise PipelineError(E_INVALID_INPUT, "Import payload must contain a shards list")
        shards: dict[str, bytes] = {}
        for item in raw_shards:
            if not isinstance(item, Mapping):
                raise PipelineError(E_INVALID_INPUT, "Each shard entry must be an object")
            name = item.get("name")
            encoded = item.get("png_base64")
            if not isinstance(name, str) or not isinstance(encoded, str):
                raise PipelineError(E_INVALID_INPUT, "Each shard needs name and png_base64")
            try:
                data = base64.b64decode(encoded.encode("ascii"), validate=True)
            except (ValueError, binascii.Error) as exc:
                raise PipelineError(
                    E_INVALID_INPUT,
                    f"Invalid base64 for shard {name}",
                    details={"filename": name},
                ) from exc
            shards[name] = data
        tasks = payload.get("tasks")
        config = payload.get("config")
        if config is not None and not isinstance(config, Mapping):
            raise PipelineError(E_INVALID_INPUT, "config must be a JSON object")
        return self.import_data(shards, tasks, config_override=config)

    def workspace_fingerprint(self) -> str:
        parts: list[dict[str, Any]] = []
        if self.tasks_path.exists():
            parts.append({"tasks": json.loads(self.tasks_path.read_text(encoding="utf-8"))})
        if self.config_path.exists():
            parts.append({"config": json.loads(self.config_path.read_text(encoding="utf-8"))})
        return sha256_json(parts)
