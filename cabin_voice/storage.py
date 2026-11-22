"""Workspace persistence, atomic checkpoints and content-addressed stage cache."""

from __future__ import annotations

import json
import os
import shutil
import wave
from pathlib import Path
from typing import Any

from .config import default_config
from .errors import CabinVoiceError
from .manifest import import_manifest, read_manifest
from .util import (
    FileLock,
    TryFileLock,
    atomic_write_bytes,
    atomic_write_json,
    canonical_bytes,
    read_json,
    sha256_bytes,
    sha256_json,
)


STAGES = ("detection", "delay", "attribution", "summary")


def _safe_remove(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


class Workspace:
    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.state_dir = self.root / ".cabin_voice"
        self.manifest_path = self.root / "manifest.json"
        self.audio_dir = self.root / "audio"
        self.sample_dir = self.root / "samples"
        self.config_path = self.state_dir / "config.json"
        self.edits_path = self.state_dir / "edits.json"
        self.overrides_path = self.state_dir / "overrides.json"
        self.run_path = self.state_dir / "run.json"
        self.checkpoint_dir = self.state_dir / "checkpoints"
        self.quarantine_dir = self.state_dir / "quarantine"
        self.cache_dir = self.state_dir / "cache"
        self.snapshot_path = self.state_dir / "last_snapshot.json"
        self.invalidation_path = self.state_dir / "last_invalidation.json"
        self.stop_path = self.state_dir / "stop.json"
        self.lock_path = self.state_dir / "workspace.lock"
        self.build_lock_path = self.state_dir / "build.lock"
        self.output_dir = self.root / "output"
        self.final_path = self.output_dir / "final.json"

    def ensure(self) -> "Workspace":
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        return self

    def lock(self) -> FileLock:
        return FileLock(self.lock_path)

    def build_lock(self) -> TryFileLock:
        return TryFileLock(self.build_lock_path)

    def import_bundle(self, manifest_source: str | Path) -> dict[str, Any]:
        self.root.mkdir(parents=True, exist_ok=True)
        target_manifest = import_manifest(manifest_source, self.root)
        # A newly imported session starts from a clean deterministic state.
        for path in (
            self.overrides_path,
            self.edits_path,
            self.run_path,
            self.snapshot_path,
            self.invalidation_path,
            self.stop_path,
            self.final_path,
        ):
            _safe_remove(path)
        _safe_remove(self.checkpoint_dir)
        _safe_remove(self.cache_dir)
        _safe_remove(self.quarantine_dir)
        self.ensure()
        if not self.config_path.exists():
            atomic_write_json(self.config_path, default_config())
        manifest = read_manifest(target_manifest)
        return {
            "workspace": str(self.root),
            "manifest": str(target_manifest),
            "session_id": manifest["session_id"],
            "chunks": len(manifest["chunks"]),
        }

    def has_manifest(self) -> bool:
        return self.manifest_path.exists()

    def require_manifest(self) -> dict[str, Any]:
        if not self.has_manifest():
            raise CabinVoiceError(
                "E_NOT_FOUND", "workspace has no imported manifest", {"workspace": str(self.root)}
            )
        return read_manifest(self.manifest_path)

    def load_config(self) -> dict[str, Any]:
        if not self.config_path.exists():
            return default_config()
        return read_json(self.config_path)

    def save_config(self, config: dict[str, Any]) -> None:
        self.ensure()
        atomic_write_json(self.config_path, config)

    def load_overrides(self) -> dict[str, Any]:
        if not self.overrides_path.exists():
            return {"schema_version": 1, "chunk_samples": {}}
        return read_json(self.overrides_path)

    def save_overrides(self, overrides: dict[str, Any]) -> None:
        atomic_write_json(self.overrides_path, overrides)

    def chunk_wav_path(self, chunk: dict[str, Any]) -> Path:
        overrides = self.load_overrides().get("chunk_samples", {})
        relative = overrides.get(str(chunk["chunk_id"]))
        if relative:
            return (self.root / relative).resolve()
        return (self.root / chunk["wav"]).resolve()

    def replace_chunk_sample_bytes(self, chunk_id: str, data: bytes) -> dict[str, Any]:
        manifest = self.require_manifest()
        chunks = {str(chunk["chunk_id"]): chunk for chunk in manifest["chunks"]}
        if chunk_id not in chunks:
            raise CabinVoiceError("E_NOT_FOUND", "unknown chunk", {"chunk_id": chunk_id})
        self.sample_dir.mkdir(parents=True, exist_ok=True)
        target = self.sample_dir / f"{chunk_id}.wav"
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        temporary.write_bytes(data)
        try:
            with wave.open(str(temporary), "rb") as handle:
                info = {
                    "channels": handle.getnchannels(),
                    "sample_width": handle.getsampwidth(),
                    "sample_rate": handle.getframerate(),
                    "frames": handle.getnframes(),
                }
        except (wave.Error, EOFError) as exc:
            temporary.unlink(missing_ok=True)
            raise CabinVoiceError("E_INVALID_WAV", "replacement WAV is invalid", {"chunk_id": chunk_id}) from exc
        expected_rate = int(manifest["sample_rate"])
        if info["channels"] != 1 or info["sample_width"] != 2 or info["sample_rate"] != expected_rate:
            temporary.unlink(missing_ok=True)
            raise CabinVoiceError(
                "E_INVALID_WAV",
                "replacement WAV must be mono 16-bit PCM at the manifest sample rate",
                {"chunk_id": chunk_id, "expected_sample_rate": expected_rate, **info},
            )
        for segment in chunks[chunk_id]["segments"]:
            if int(segment["offset_samples"]) + int(segment["sample_count"]) > info["frames"]:
                temporary.unlink(missing_ok=True)
                raise CabinVoiceError(
                    "E_ZONE_OFFSET_OUT_OF_BOUNDS",
                    "replacement sample is shorter than a declared zone segment",
                    {
                        "chunk_id": chunk_id,
                        "zone_id": segment["zone_id"],
                        "parameter": "offset_samples",
                        "wav_frames": info["frames"],
                    },
                )
        os.replace(temporary, target)
        overrides = self.load_overrides()
        overrides.setdefault("chunk_samples", {})[chunk_id] = f"samples/{chunk_id}.wav"
        self.save_overrides(overrides)
        return {
            "chunk_id": chunk_id,
            "sample_path": str(target),
            "sha256": sha256_bytes(data),
            **info,
        }

    def replace_chunk_sample_file(self, chunk_id: str, source: str | Path) -> dict[str, Any]:
        try:
            data = Path(source).read_bytes()
        except FileNotFoundError as exc:
            raise CabinVoiceError("E_NOT_FOUND", "replacement WAV not found", {"path": str(source)}) from exc
        return self.replace_chunk_sample_bytes(chunk_id, data)

    def state_fingerprint(self) -> str:
        config = self.load_config()
        edits_bytes = self.edits_path.read_bytes() if self.edits_path.exists() else b"{}"
        overrides_bytes = self.overrides_path.read_bytes() if self.overrides_path.exists() else b"{}"
        manifest_bytes = self.manifest_path.read_bytes() if self.manifest_path.exists() else b"{}"
        return sha256_json(
            {
                "manifest": sha256_bytes(manifest_bytes),
                "config": sha256_json(config),
                "edits": sha256_bytes(edits_bytes),
                "overrides": sha256_bytes(overrides_bytes),
            }
        )

    def request_stop(self) -> dict[str, Any]:
        self.ensure()
        payload = {"requested": True, "reason": "graceful_stop"}
        atomic_write_json(self.stop_path, payload)
        return payload

    def clear_stop(self) -> None:
        self.stop_path.unlink(missing_ok=True)

    def stop_requested(self) -> bool:
        if not self.stop_path.exists():
            return False
        try:
            return bool(read_json(self.stop_path).get("requested"))
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            return False

    def write_final(self, final: dict[str, Any]) -> bytes:
        data = canonical_bytes(final)
        atomic_write_bytes(self.final_path, data)
        return data

    def read_final_bytes(self) -> bytes | None:
        if not self.final_path.exists():
            return None
        return self.final_path.read_bytes()

    def load_edits_state(self) -> dict[str, Any]:
        if not self.edits_path.exists():
            return {
                "schema_version": 1,
                "edit_version": 0,
                "activity_ops": [],
                "priority_overrides": {},
                "lexicon_overrides": {},
                "trail": [],
            }
        return read_json(self.edits_path)

    def clone_inputs_to(self, destination: str | Path) -> "Workspace":
        """Clone only source inputs and deterministic edit/config state for a reference run."""

        target = Workspace(destination).ensure()
        target.root.mkdir(parents=True, exist_ok=True)
        for name in ("manifest.json", "audio", "samples"):
            source = self.root / name
            if source.is_dir():
                shutil.copytree(source, target.root / name, dirs_exist_ok=True)
            elif source.exists():
                shutil.copyfile(source, target.root / name)
        target.state_dir.mkdir(parents=True, exist_ok=True)
        for source, destination_path in (
            (self.config_path, target.config_path),
            (self.edits_path, target.edits_path),
            (self.overrides_path, target.overrides_path),
        ):
            if source.exists():
                shutil.copyfile(source, destination_path)
        target.ensure()
        return target


class CheckpointLog:
    """Monotonic per-run checkpoint log guarded by an OS file lock."""

    def __init__(self, workspace: Workspace):
        self.workspace = workspace
        self.directory = workspace.checkpoint_dir
        self.directory.mkdir(parents=True, exist_ok=True)

    def _record_with_checksum(self, body: dict[str, Any]) -> dict[str, Any]:
        record = dict(body)
        record.pop("checksum", None)
        record["checksum"] = sha256_json(record)
        return record

    def _validate_record(self, path: Path, payload: Any) -> tuple[dict[str, Any] | None, str | None]:
        if not isinstance(payload, dict):
            return None, "not_an_object"
        required = {
            "schema_version",
            "build_id",
            "seq",
            "position",
            "chunk_id",
            "chunk_index",
            "stage",
            "stage_index",
            "content_fingerprint",
            "input_fingerprint",
            "config_version",
            "edit_version",
            "fingerprint_components",
            "artifact_summary",
            "artifact",
            "checksum",
        }
        missing = sorted(required - set(payload))
        if missing:
            return None, f"missing:{','.join(missing)}"
        checksum = payload.get("checksum")
        body = dict(payload)
        body.pop("checksum", None)
        if checksum != sha256_json(body):
            return None, "checksum_mismatch"
        if payload.get("stage") not in STAGES:
            return None, "invalid_stage"
        if payload.get("stage_index") != STAGES.index(payload["stage"]) + 1:
            return None, "invalid_stage_index"
        if not isinstance(payload.get("seq"), int) or payload["seq"] < 1:
            return None, "invalid_seq"
        return payload, None

    def _scan_unlocked(self) -> dict[str, Any]:
        candidates: dict[int, list[dict[str, Any]]] = {}
        invalid: list[dict[str, Any]] = []
        paths: dict[int, list[Path]] = {}
        for path in sorted(self.directory.glob("*.json")):
            try:
                payload = read_json(path)
            except (json.JSONDecodeError, UnicodeDecodeError, OSError):
                invalid.append({"path": str(path), "reason": "unreadable_json"})
                continue
            record, reason = self._validate_record(path, payload)
            if record is None:
                invalid.append({"path": str(path), "reason": reason, "seq": payload.get("seq") if isinstance(payload, dict) else None})
                continue
            seq = int(record["seq"])
            candidates.setdefault(seq, []).append(record)
            paths.setdefault(seq, []).append(path)

        unique: dict[int, dict[str, Any]] = {}
        unique_paths: dict[int, Path] = {}
        for seq, records in candidates.items():
            if len(records) != 1:
                for path in paths[seq]:
                    invalid.append({"path": str(path), "reason": "duplicate_seq", "seq": seq})
                continue
            unique[seq] = records[0]
            unique_paths[seq] = paths[seq][0]

        run = self.load_run()
        plan = run.get("plan", []) if run else []
        prefix: list[dict[str, Any]] = []
        prefix_paths: list[Path] = []
        expected_seq = 1
        while expected_seq in unique:
            record = unique[expected_seq]
            if expected_seq > len(plan):
                invalid.append({"path": str(unique_paths[expected_seq]), "reason": "beyond_plan", "seq": expected_seq})
                break
            expected = plan[expected_seq - 1]
            if (
                int(record.get("position", -1)) != expected_seq
                or record.get("chunk_id") != expected["chunk_id"]
                or record.get("stage") != expected["stage"]
                or int(record.get("stage_index", -1)) != int(expected["stage_index"])
            ):
                invalid.append(
                    {
                        "path": str(unique_paths[expected_seq]),
                        "reason": "cross_chunk_or_stage_order",
                        "seq": expected_seq,
                    }
                )
                break
            prefix.append(record)
            prefix_paths.append(unique_paths[expected_seq])
            expected_seq += 1

        for seq, record in sorted(unique.items()):
            if seq >= expected_seq:
                invalid.append(
                    {
                        "path": str(unique_paths[seq]),
                        "reason": "non_contiguous_or_after_invalid",
                        "seq": seq,
                    }
                )
        return {
            "run": run,
            "prefix": prefix,
            "prefix_paths": prefix_paths,
            "invalid": invalid,
            "all_unique": unique,
        }

    def load_consistent(self) -> dict[str, Any]:
        with self.workspace.lock():
            return self._scan_unlocked()

    def recover_prefix(self, *, prune: bool = True) -> dict[str, Any]:
        with self.workspace.lock():
            scan = self._scan_unlocked()
            if prune:
                keep = {path.resolve() for path in scan["prefix_paths"]}
                self.workspace.quarantine_dir.mkdir(parents=True, exist_ok=True)
                for path in sorted(self.directory.glob("*.json")):
                    if path.resolve() in keep:
                        continue
                    destination = self.workspace.quarantine_dir / path.name
                    counter = 1
                    while destination.exists():
                        destination = self.workspace.quarantine_dir / f"{path.stem}.{counter}{path.suffix}"
                        counter += 1
                    shutil.move(str(path), str(destination))
            return scan

    def begin_run(
        self,
        *,
        plan: list[dict[str, Any]],
        metadata: dict[str, Any],
        reset: bool,
        clear_cache: bool,
    ) -> dict[str, Any]:
        with self.workspace.lock():
            if reset:
                for path in self.directory.glob("*.json"):
                    path.unlink(missing_ok=True)
                if clear_cache and self.workspace.cache_dir.exists():
                    shutil.rmtree(self.workspace.cache_dir)
                self.workspace.cache_dir.mkdir(parents=True, exist_ok=True)
            self.workspace.stop_path.unlink(missing_ok=True)
            run = {
                "schema_version": 1,
                **metadata,
                "plan": plan,
            }
            atomic_write_json(self.workspace.run_path, run)
            return run

    def load_run(self) -> dict[str, Any] | None:
        if not self.workspace.run_path.exists():
            return None
        try:
            return read_json(self.workspace.run_path)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return None

    def publish(
        self,
        body: dict[str, Any],
        *,
        expected_position: int,
    ) -> dict[str, Any]:
        with self.workspace.lock():
            run = self.load_run()
            if run is None:
                raise CabinVoiceError("E_STALE_CHECKPOINT", "no active run metadata", {"expected_position": expected_position})
            scan = self._scan_unlocked()
            next_position = len(scan["prefix"]) + 1
            if expected_position != next_position:
                raise CabinVoiceError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint publication is behind or ahead of the contiguous prefix",
                    {"expected_position": expected_position, "next_position": next_position},
                )
            if int(body.get("seq", -1)) != next_position:
                raise CabinVoiceError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint sequence is not the next contiguous value",
                    {"seq": body.get("seq"), "next_position": next_position},
                )
            plan = run["plan"]
            if next_position > len(plan):
                raise CabinVoiceError(
                    "E_STALE_CHECKPOINT", "checkpoint exceeds run plan", {"position": next_position}
                )
            expected = plan[next_position - 1]
            if (
                body.get("chunk_id") != expected["chunk_id"]
                or body.get("stage") != expected["stage"]
                or int(body.get("stage_index", -1)) != int(expected["stage_index"])
                or body.get("build_id") != run.get("build_id")
            ):
                raise CabinVoiceError(
                    "E_STALE_CHECKPOINT",
                    "cross-chunk, cross-stage or cross-run publication rejected",
                    {
                        "position": next_position,
                        "expected": expected,
                        "received": {
                            "chunk_id": body.get("chunk_id"),
                            "stage": body.get("stage"),
                            "stage_index": body.get("stage_index"),
                            "build_id": body.get("build_id"),
                        },
                    },
                )
            path = self.directory / f"{next_position:012d}.json"
            if path.exists():  # Defensive: the module-level contiguous prefix already excludes it.
                raise CabinVoiceError(
                    "E_STALE_CHECKPOINT",
                    "checkpoint sequence already exists",
                    {"seq": next_position, "path": str(path)},
                )
            record = self._record_with_checksum(body)
            atomic_write_json(path, record)
            return record

    def current_records(self) -> list[dict[str, Any]]:
        return self.load_consistent()["prefix"]


class StageCache:
    def __init__(self, workspace: Workspace):
        self.workspace = workspace
        self.root = workspace.cache_dir
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, stage: str, chunk_id: str, fingerprint: str) -> Path:
        return self.root / stage / chunk_id / f"{fingerprint}.json"

    def get(self, stage: str, chunk_id: str, fingerprint: str) -> dict[str, Any] | None:
        path = self._path(stage, chunk_id, fingerprint)
        if not path.exists():
            return None
        try:
            payload = read_json(path)
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            return None
        if payload.get("input_fingerprint") != fingerprint:
            return None
        artifact = payload.get("artifact")
        if sha256_json(artifact) != payload.get("artifact_summary"):
            return None
        return payload

    def put(
        self,
        stage: str,
        chunk_id: str,
        fingerprint: str,
        *,
        artifact: dict[str, Any],
        artifact_summary: str,
        components: dict[str, str],
    ) -> None:
        payload = {
            "schema_version": 1,
            "stage": stage,
            "chunk_id": chunk_id,
            "input_fingerprint": fingerprint,
            "artifact_summary": artifact_summary,
            "components": components,
            "artifact": artifact,
        }
        atomic_write_json(self._path(stage, chunk_id, fingerprint), payload)
