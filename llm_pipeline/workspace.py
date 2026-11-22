"""Filesystem layout, configuration persistence, and JSONL shard handling."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from .config import config_digest, default_config, validate_config
from .errors import E_INVALID_INPUT, E_IO, E_NOT_FOUND, PipelineError, invalid_input
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    canonical_json_line,
    ensure_no_path_traversal,
    file_lock,
    read_json,
    sha256_bytes,
)


CONFIG_NAME = "config.json"
DATA_DIR = "data"
SHARDS_DIR = "data/shards"
STATE_DIR = ".pipeline"


class Workspace:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root).resolve()
        self.config_path = self.root / CONFIG_NAME
        self.data_dir = self.root / DATA_DIR
        self.shards_dir = self.root / SHARDS_DIR
        self.state_dir = self.root / STATE_DIR
        self.checkpoint_dir = self.state_dir / "checkpoints"
        self.artifact_dir = self.state_dir / "artifacts"
        self.quarantine_dir = self.state_dir / "checkpoints_quarantine"
        self.lock_dir = self.state_dir / "locks"
        self.tmp_dir = self.state_dir / "tmp"
        self.stop_path = self.state_dir / "stop.json"
        self.last_run_path = self.state_dir / "last_run.json"
        self.publish_lock_path = self.lock_dir / "publish.lock"
        self.config_lock_path = self.lock_dir / "config.lock"

    def ensure(self) -> None:
        for path in (
            self.root,
            self.shards_dir,
            self.checkpoint_dir,
            self.artifact_dir,
            self.quarantine_dir,
            self.lock_dir,
            self.tmp_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def initialized(self) -> bool:
        return self.config_path.is_file() and self.shards_dir.is_dir()

    def require_initialized(self) -> None:
        if not self.config_path.is_file():
            raise PipelineError(
                E_INVALID_INPUT,
                "workspace is not initialized (missing config.json)",
                {"workspace": str(self.root)},
                http_status=400,
            )

    def initialize(self, config: dict[str, Any] | None = None) -> None:
        self.ensure()
        config = validate_config(config if config is not None else default_config())
        if not self.config_path.exists():
            atomic_write_json(self.config_path, config)

    def initialize_demo(
        self,
        demo_dir: str | Path,
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        from .generator import generate_demo_assets

        self.ensure()
        existing = [
            path
            for path in (self.config_path, self.shards_dir)
            if path.exists() and (path != self.shards_dir or any(path.iterdir()))
        ]
        if existing and not force:
            raise PipelineError(
                E_INVALID_INPUT,
                "workspace already contains demo data; use --force to replace it",
                {"workspace": str(self.root), "existing": [str(p) for p in existing]},
                http_status=409,
            )
        demo_path = Path(demo_dir).resolve()
        manifest = generate_demo_assets(demo_path)
        if force:
            if self.shards_dir.exists():
                shutil.rmtree(self.shards_dir)
            self.shards_dir.mkdir(parents=True, exist_ok=True)
        else:
            self.shards_dir.mkdir(parents=True, exist_ok=True)
        for shard in sorted((demo_path / "shards").glob("*.jsonl")):
            target = self.shards_dir / shard.name
            atomic_write_bytes(target, shard.read_bytes())
        config = validate_config(read_json(demo_path / "config.json"))
        atomic_write_json(self.config_path, config)
        return manifest

    def load_config(self, *, require: bool = True) -> dict[str, Any]:
        if not self.config_path.exists():
            if require:
                self.require_initialized()
            return validate_config(default_config())
        try:
            raw = json.loads(self.config_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineError(
                E_INVALID_INPUT,
                "config.json is not valid JSON",
                {"path": str(self.config_path), "error": str(exc)},
                http_status=400,
            ) from exc
        return validate_config(raw)

    def save_config(self, config: dict[str, Any], *, bump_revision: bool = True) -> dict[str, Any]:
        self.ensure()
        with file_lock(self.config_lock_path):
            current = self.load_config(require=False)
            validated = validate_config(config)
            validated["revision"] = (
                max(current.get("revision", 0), validated.get("revision", 0)) + 1
                if bump_revision
                else validated.get("revision", current.get("revision", 0))
            )
            atomic_write_json(self.config_path, validated)
        return validated

    def replace_config(self, config: dict[str, Any]) -> dict[str, Any]:
        self.ensure()
        with file_lock(self.config_lock_path):
            current = self.load_config(require=False)
            validated = validate_config(config)
            if canonical_json_bytes(validated) == canonical_json_bytes(current):
                return current
            validated["revision"] = current.get("revision", 0) + 1
            atomic_write_json(self.config_path, validated)
        return validated

    def config_version(self, config: dict[str, Any] | None = None) -> str:
        return config_digest(config if config is not None else self.load_config())

    def shard_paths(self) -> list[Path]:
        if not self.shards_dir.exists():
            return []
        return sorted(
            (path for path in self.shards_dir.iterdir() if path.is_file() and path.suffix == ".jsonl"),
            key=lambda path: path.name,
        )

    def shard_fingerprints(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        for path in self.shard_paths():
            raw = path.read_bytes()
            result.append(
                {
                    "name": path.name,
                    "digest": sha256_bytes(raw),
                    "size": len(raw),
                }
            )
        return result

    def load_shards(self) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Read all shards and return (shard metadata, records)."""
        paths = self.shard_paths()
        if not paths:
            raise PipelineError(
                E_INVALID_INPUT,
                "workspace contains no JSONL shards",
                {"shards_dir": str(self.shards_dir)},
                http_status=400,
            )
        shards: list[dict[str, Any]] = []
        records: list[dict[str, Any]] = []
        seen_ids: dict[str, tuple[str, int]] = {}
        for path in paths:
            try:
                raw = path.read_bytes()
            except OSError as exc:
                raise PipelineError(
                    E_IO,
                    "failed to read shard",
                    {"path": str(path), "error": str(exc)},
                    http_status=500,
                ) from exc
            shard_records = self._parse_shard(path.name, raw)
            shards.append(
                {
                    "name": path.name,
                    "digest": sha256_bytes(raw),
                    "size": len(raw),
                    "record_count": len(shard_records),
                }
            )
            for record in shard_records:
                sample_id = record["id"]
                if sample_id in seen_ids:
                    previous_shard, previous_line = seen_ids[sample_id]
                    raise PipelineError(
                        E_INVALID_INPUT,
                        f"duplicate sample id across shards: {sample_id}",
                        {
                            "id": sample_id,
                            "first": {"shard": previous_shard, "line": previous_line},
                            "second": {"shard": path.name, "line": record["_line"]},
                        },
                        http_status=400,
                    )
                seen_ids[sample_id] = (path.name, record["_line"])
                records.append(record)
        return shards, records

    @staticmethod
    def _parse_shard(name: str, raw: bytes) -> list[dict[str, Any]]:
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PipelineError(
                E_INVALID_INPUT,
                "shard is not valid UTF-8",
                {"shard": name, "error": str(exc)},
                http_status=400,
            ) from exc
        records: list[dict[str, Any]] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                raise PipelineError(
                    E_INVALID_INPUT,
                    "invalid JSONL record",
                    {"shard": name, "line": line_number, "error": str(exc)},
                    http_status=400,
                ) from exc
            if not isinstance(item, dict):
                raise PipelineError(
                    E_INVALID_INPUT,
                    "JSONL record must be an object",
                    {"shard": name, "line": line_number},
                    http_status=400,
                )
            for field in ("id", "source", "lang", "text"):
                if field not in item:
                    raise PipelineError(
                        E_INVALID_INPUT,
                        f"JSONL record is missing required field {field}",
                        {"shard": name, "line": line_number, "field": field},
                        http_status=400,
                    )
            for field in ("id", "source", "lang"):
                if not isinstance(item[field], str) or not item[field]:
                    raise PipelineError(
                        E_INVALID_INPUT,
                        f"JSONL field {field} must be a non-empty string",
                        {"shard": name, "line": line_number, "field": field},
                        http_status=400,
                    )
            if not isinstance(item["text"], str):
                raise PipelineError(
                    E_INVALID_INPUT,
                    "JSONL field text must be a string",
                    {"shard": name, "line": line_number},
                    http_status=400,
                )
            records.append(
                {
                    "id": item["id"],
                    "source": item["source"],
                    "lang": item["lang"],
                    "text": item["text"],
                    "_shard": name,
                    "_line": line_number,
                }
            )
        return records

    def validate_shard_bytes(self, name: str, raw: bytes) -> list[dict[str, Any]]:
        return self._parse_shard(name, raw)

    def import_shard(
        self,
        name: str,
        raw: bytes,
        *,
        replace: bool = False,
        validate_globally: bool = True,
    ) -> dict[str, Any]:
        try:
            ensure_no_path_traversal(name)
        except ValueError as exc:
            raise invalid_input(str(exc), {"name": name}) from exc
        if not name.endswith(".jsonl"):
            raise invalid_input("shard name must end with .jsonl", {"name": name})
        self.ensure()
        parsed = self.validate_shard_bytes(name, raw)
        target = self.shards_dir / name
        exists = target.exists()
        if exists and not replace:
            raise PipelineError(
                E_INVALID_INPUT,
                "shard already exists; use replace-shard or replace=true",
                {"name": name},
                http_status=409,
            )
        if replace and not exists:
            raise PipelineError(
                E_NOT_FOUND,
                "shard does not exist; use import-shard for a new shard",
                {"name": name},
                http_status=404,
            )
        old_bytes = target.read_bytes() if exists else None
        old_digest = sha256_bytes(old_bytes) if old_bytes is not None else None
        new_digest = sha256_bytes(raw)
        if old_digest == new_digest:
            return {
                "changed": False,
                "name": name,
                "digest": new_digest,
                "record_count": len(parsed),
                "operation": "replace" if replace else "import",
            }
        atomic_write_bytes(target, raw)
        if validate_globally:
            try:
                self.load_shards()
            except Exception:
                if old_bytes is not None:
                    atomic_write_bytes(target, old_bytes)
                else:
                    try:
                        target.unlink()
                    except FileNotFoundError:
                        pass
                raise
        return {
            "changed": True,
            "name": name,
            "digest": new_digest,
            "old_digest": old_digest,
            "record_count": len(parsed),
            "operation": "replace" if replace else "import",
        }

    def import_shard_from_path(
        self,
        name: str,
        source_path: str | Path,
        *,
        replace: bool = False,
        source_content: str | None = None,
    ) -> dict[str, Any]:
        if source_path is not None:
            try:
                raw = Path(source_path).read_bytes()
            except OSError as exc:
                raise PipelineError(
                    E_INVALID_INPUT,
                    "failed to read source shard",
                    {"path": str(source_path), "error": str(exc)},
                    http_status=400,
                ) from exc
        else:
            assert source_content is not None
            raw = source_content.encode("utf-8")
        return self.import_shard(name, raw, replace=replace)

    def request_stop(self, reason: str = "user") -> dict[str, Any]:
        self.ensure()
        payload = {"requested": True, "reason": reason}
        atomic_write_json(self.stop_path, payload)
        return payload

    def stop_requested(self) -> bool:
        if not self.stop_path.exists():
            return False
        try:
            data = json.loads(self.stop_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return True
        return bool(data.get("requested", True))

    def clear_stop(self) -> None:
        try:
            self.stop_path.unlink()
        except FileNotFoundError:
            return
        try:
            fd = os.open(self.stop_path.parent, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        except OSError:
            pass

    def stop_status(self) -> dict[str, Any]:
        if not self.stop_path.exists():
            return {"requested": False}
        data = read_json(self.stop_path, {})
        return {"requested": True, "request": data}

    def write_last_run(self, payload: dict[str, Any]) -> None:
        self.ensure()
        atomic_write_json(self.last_run_path, payload)

    def last_run(self) -> dict[str, Any] | None:
        return read_json(self.last_run_path, None)

    def checkpoint_files(self) -> list[Path]:
        if not self.checkpoint_dir.exists():
            return []
        return sorted(
            (path for path in self.checkpoint_dir.iterdir() if path.is_file() and path.suffix == ".json"),
            key=lambda path: path.name,
        )

    def artifact_path(self, stage: str, digest: str) -> Path:
        return self.artifact_dir / stage / f"{digest}.json"

    def write_artifact_if_missing(self, stage: str, digest: str, data: bytes) -> Path:
        path = self.artifact_path(stage, digest)
        if path.exists():
            return path
        atomic_write_bytes(path, data)
        return path


def record_keep_product_bytes(payload: dict[str, Any]) -> bytes:
    rows = []
    for item in sorted(payload["kept"], key=lambda row: row["id"]):
        rows.append(
            {
                "id": item["id"],
                "source": item["source"],
                "lang": item["lang"],
                "text": item["text"],
                "normalized_text": item["normalized_text"],
                "simhash": item["simhash"],
            }
        )
    return b"".join(canonical_json_line(row) for row in rows)


def allocation_product_bytes(payload: dict[str, Any]) -> bytes:
    rows = []
    for layer in sorted(payload["layers"], key=lambda row: (row["source"], row["lang"])):
        rows.append(
            {
                "layer_id": layer["layer_id"],
                "source": layer["source"],
                "lang": layer["lang"],
                "weight": layer["weight"],
                "available": layer["available"],
                "allocated": layer["allocated"],
                "status": layer["status"],
            }
        )
    return b"".join(canonical_json_line(row) for row in rows)


def mixed_product_bytes(payload: dict[str, Any]) -> bytes:
    rows = []
    for row in payload["rows"]:
        rows.append(
            {
                "id": row["id"],
                "source": row["source"],
                "lang": row["lang"],
                "text": row["text"],
            }
        )
    return b"".join(canonical_json_line(row) for row in rows)
