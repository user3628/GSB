from __future__ import annotations

import hashlib
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Sequence

from .config import ManagerConfig
from .models import ParsedCheckpointRequest, ShardKind, canonical_json, utc_now


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _fsync_directory(path: Path) -> None:
    # Directory fsync is available on POSIX. Windows persists the file metadata
    # through the file flush/rename path and may reject opening directories.
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


@dataclass(frozen=True, slots=True)
class PreparedCheckpoint:
    checkpoint_id: str
    temp_dir: Path
    final_dir: Path
    manifest: dict[str, Any]
    manifest_bytes: bytes
    manifest_sha256: str
    relative_shards: tuple[dict[str, Any], ...]


class CheckpointStorage:
    def __init__(self, config: ManagerConfig) -> None:
        self.config = config
        self.data_dir = config.data_dir
        config.checkpoints_dir.mkdir(parents=True, exist_ok=True)
        config.temp_dir.mkdir(parents=True, exist_ok=True)
        config.intents_dir.mkdir(parents=True, exist_ok=True)

    def final_dir(self, checkpoint_id: str) -> Path:
        return self.config.checkpoints_dir / checkpoint_id

    def temp_dir(self, checkpoint_id: str) -> Path:
        return self.config.temp_dir / checkpoint_id

    def relative_manifest_path(self, checkpoint_id: str) -> str:
        return (Path("checkpoints") / checkpoint_id / "manifest.json").as_posix()

    def write_temp_checkpoint(
        self,
        checkpoint_id: str,
        parsed: ParsedCheckpointRequest,
        on_first_shard: Callable[[Path], None] | None = None,
    ) -> PreparedCheckpoint:
        temp_dir = self.temp_dir(checkpoint_id)
        final_dir = self.final_dir(checkpoint_id)
        if temp_dir.exists():
            shutil.rmtree(temp_dir)
        if final_dir.exists():
            raise OSError(f"checkpoint destination already exists: {final_dir}")
        temp_dir.mkdir(parents=True)
        relative_shards: list[dict[str, Any]] = []
        first = True
        for shard in parsed.all_shards:
            relative_path = Path(shard.kind.value) / f"shard-{shard.ordinal:05d}.bin"
            target = temp_dir / relative_path
            _write_bytes(target, shard.data)
            relative_shards.append(
                {
                    "kind": shard.kind.value,
                    "ordinal": shard.ordinal,
                    "logical_name": shard.name,
                    "relative_path": relative_path.as_posix(),
                    "size_bytes": len(shard.data),
                    "sha256": shard.sha256,
                }
            )
            if first and on_first_shard is not None:
                first = False
                on_first_shard(target)
            elif self.config.writer_delay_seconds:
                import time

                time.sleep(self.config.writer_delay_seconds)
        manifest = {
            "format_version": 1,
            "checkpoint_id": checkpoint_id,
            "request_id": parsed.request_id,
            "round": parsed.round_number,
            "scheduler_position": parsed.scheduler_position,
            "data_cursor": parsed.data_cursor,
            "created_at": utc_now(),
            "params": [item for item in relative_shards if item["kind"] == ShardKind.PARAMS.value],
            "optimizer": [item for item in relative_shards if item["kind"] == ShardKind.OPTIMIZER.value],
        }
        manifest_bytes = (canonical_json(manifest) + "\n").encode("utf-8")
        manifest_path = temp_dir / "manifest.json"
        _write_bytes(manifest_path, manifest_bytes)
        _fsync_directory(temp_dir)
        return PreparedCheckpoint(
            checkpoint_id=checkpoint_id,
            temp_dir=temp_dir,
            final_dir=final_dir,
            manifest=manifest,
            manifest_bytes=manifest_bytes,
            manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            relative_shards=tuple(relative_shards),
        )

    def verify_temp_checkpoint(self, prepared: PreparedCheckpoint) -> dict[str, Any]:
        mismatches: list[dict[str, Any]] = []
        manifest_path = prepared.temp_dir / "manifest.json"
        if not manifest_path.is_file():
            mismatches.append({"scope": "manifest", "error": "manifest_missing"})
        else:
            actual_manifest_sha = sha256_file(manifest_path)
            if actual_manifest_sha != prepared.manifest_sha256:
                mismatches.append(
                    {
                        "scope": "manifest",
                        "error": "manifest_digest_mismatch",
                        "expected_sha256": prepared.manifest_sha256,
                        "actual_sha256": actual_manifest_sha,
                    }
                )
        params_total = params_verified = 0
        optimizer_total = optimizer_verified = 0
        for shard in prepared.relative_shards:
            if shard["kind"] == ShardKind.PARAMS.value:
                params_total += 1
            else:
                optimizer_total += 1
            path = prepared.temp_dir / shard["relative_path"]
            error = None
            actual_sha = None
            if not path.is_file():
                error = "shard_missing"
            else:
                if path.stat().st_size != shard["size_bytes"]:
                    error = "size_mismatch"
                actual_sha = sha256_file(path)
                if actual_sha != shard["sha256"]:
                    error = "digest_mismatch"
            if error is None:
                if shard["kind"] == ShardKind.PARAMS.value:
                    params_verified += 1
                else:
                    optimizer_verified += 1
            else:
                mismatches.append(
                    {
                        "scope": "shard",
                        "kind": shard["kind"],
                        "ordinal": shard["ordinal"],
                        "relative_path": shard["relative_path"],
                        "error": error,
                        "expected_sha256": shard["sha256"],
                        "actual_sha256": actual_sha,
                    }
                )
        return {
            "valid": not mismatches,
            "manifest_sha256": prepared.manifest_sha256,
            "params": {"total": params_total, "verified": params_verified},
            "optimizer": {"total": optimizer_total, "verified": optimizer_verified},
            "mismatches": mismatches,
        }

    def corrupt_first_temp_shard(self, prepared: PreparedCheckpoint) -> Path | None:
        if not prepared.relative_shards:
            return None
        path = prepared.temp_dir / prepared.relative_shards[0]["relative_path"]
        if not path.is_file() or path.stat().st_size == 0:
            return path
        with path.open("r+b") as handle:
            original = handle.read(1)
            handle.seek(0)
            handle.write(bytes([original[0] ^ 0xFF]))
            handle.flush()
            os.fsync(handle.fileno())
        return path

    def promote(self, prepared: PreparedCheckpoint) -> None:
        os.replace(prepared.temp_dir, prepared.final_dir)
        _fsync_directory(self.config.checkpoints_dir)

    def cleanup_checkpoint_artifacts(self, checkpoint_id: str) -> list[str]:
        removed: list[str] = []
        for path in (self.temp_dir(checkpoint_id), self.final_dir(checkpoint_id)):
            if path.exists():
                shutil.rmtree(path)
                removed.append(str(path))
        return removed

    def write_intent_file(self, intent_id: str, payload: dict[str, Any]) -> Path:
        path = self.config.intents_dir / f"{intent_id}.json"
        temp = path.with_suffix(".json.tmp")
        _write_bytes(temp, (canonical_json(payload) + "\n").encode("utf-8"))
        os.replace(temp, path)
        _fsync_directory(self.config.intents_dir)
        return path

    def intent_files(self) -> dict[str, Path]:
        return {path.stem: path for path in self.config.intents_dir.glob("*.json")}

    def intent_temp_files(self) -> list[Path]:
        return sorted(self.config.intents_dir.glob("*.json.tmp"))

    def cleanup_orphans(self, committed_ids: set[str]) -> dict[str, list[str]]:
        removed_temporary: list[str] = []
        removed_checkpoint_dirs: list[str] = []
        for path in self.config.temp_dir.iterdir():
            if path.is_dir():
                shutil.rmtree(path)
                removed_temporary.append(path.name)
            else:
                path.unlink(missing_ok=True)
                removed_temporary.append(path.name)
        for path in self.config.checkpoints_dir.iterdir():
            if not path.is_dir() or path.name in committed_ids:
                continue
            shutil.rmtree(path)
            removed_checkpoint_dirs.append(path.name)
        return {
            "temporary_directories": sorted(removed_temporary),
            "orphan_checkpoint_directories": sorted(removed_checkpoint_dirs),
        }

    def verify_committed_checkpoint(self, checkpoint: dict[str, Any]) -> dict[str, Any]:
        checkpoint_id = checkpoint["checkpoint_id"]
        mismatches: list[dict[str, Any]] = []
        manifest_path = self.data_dir / checkpoint["manifest_path"] if checkpoint.get("manifest_path") else None
        manifest_payload: dict[str, Any] | None = None
        if manifest_path is None or not manifest_path.is_file():
            mismatches.append({"scope": "manifest", "error": "manifest_missing"})
        else:
            actual_manifest_sha = sha256_file(manifest_path)
            if actual_manifest_sha != checkpoint.get("manifest_sha256"):
                mismatches.append(
                    {
                        "scope": "manifest",
                        "error": "manifest_digest_mismatch",
                        "expected_sha256": checkpoint.get("manifest_sha256"),
                        "actual_sha256": actual_manifest_sha,
                    }
                )
            try:
                manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                mismatches.append({"scope": "manifest", "error": "manifest_unreadable", "message": str(exc)})
        root = self.final_dir(checkpoint_id)
        db_shards: Sequence[dict[str, Any]] = checkpoint.get("shards", [])
        db_by_key = {(shard["kind"], shard["ordinal"]): shard for shard in db_shards}
        manifest_entries: list[dict[str, Any]] = []
        manifest_by_key: dict[tuple[str, int], dict[str, Any]] = {}
        if manifest_payload is not None:
            if manifest_payload.get("checkpoint_id") != checkpoint_id:
                mismatches.append({"scope": "manifest", "error": "checkpoint_id_mismatch"})
            if manifest_payload.get("round") != checkpoint.get("round"):
                mismatches.append({"scope": "manifest", "error": "round_mismatch"})
            if manifest_payload.get("scheduler_position") != checkpoint.get("scheduler_position"):
                mismatches.append({"scope": "manifest", "error": "scheduler_position_mismatch"})
            entries = list(manifest_payload.get("params", [])) + list(manifest_payload.get("optimizer", []))
            for entry in entries:
                try:
                    key = (str(entry["kind"]), int(entry["ordinal"]))
                except (KeyError, TypeError, ValueError):
                    mismatches.append({"scope": "manifest", "error": "malformed_shard_entry"})
                    continue
                manifest_entries.append(entry)
                manifest_by_key[key] = entry
        params_total = params_verified = 0
        optimizer_total = optimizer_verified = 0
        for key, db_shard in db_by_key.items():
            kind, ordinal = key
            if kind == ShardKind.PARAMS.value:
                params_total += 1
            else:
                optimizer_total += 1
            manifest_entry = manifest_by_key.get(key)
            error = None
            actual_sha = None
            if manifest_entry is None:
                error = "missing_from_manifest"
            elif manifest_entry.get("sha256") != db_shard["sha256"]:
                error = "manifest_digest_mismatch"
            elif manifest_entry.get("relative_path") != db_shard["relative_path"]:
                error = "manifest_path_mismatch"
            if error is None:
                path = root / db_shard["relative_path"]
                try:
                    resolved = path.resolve()
                    if not resolved.is_relative_to(root.resolve()):
                        error = "path_escape"
                except OSError:
                    error = "path_unresolvable"
                if error is None and not path.is_file():
                    error = "shard_missing"
                if error is None:
                    if path.stat().st_size != db_shard["size_bytes"]:
                        error = "size_mismatch"
                    actual_sha = sha256_file(path)
                    if actual_sha != db_shard["sha256"]:
                        error = "digest_mismatch"
            if error is None:
                if kind == ShardKind.PARAMS.value:
                    params_verified += 1
                else:
                    optimizer_verified += 1
            else:
                mismatches.append(
                    {
                        "scope": "shard",
                        "kind": kind,
                        "ordinal": ordinal,
                        "relative_path": db_shard.get("relative_path"),
                        "error": error,
                        "expected_sha256": db_shard.get("sha256"),
                        "actual_sha256": actual_sha,
                    }
                )
        missing_db_entries = sorted(set(manifest_by_key) - set(db_by_key))
        for kind, ordinal in missing_db_entries:
            mismatches.append({"scope": "shard", "kind": kind, "ordinal": ordinal, "error": "missing_from_database"})
        return {
            "valid": not mismatches,
            "manifest": {
                "path": checkpoint.get("manifest_path"),
                "sha256": checkpoint.get("manifest_sha256"),
                "valid": not any(item.get("scope") == "manifest" for item in mismatches),
            },
            "params": {"total": params_total, "verified": params_verified},
            "optimizer": {"total": optimizer_total, "verified": optimizer_verified},
            "mismatches": mismatches,
        }

