from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import Any

from . import SCHEMA_VERSION
from .config import STAGE_LABELS, STAGES
from .errors import InvalidStage, StaleCheckpoint, VerifyFailed
from .util import (
    FileLock,
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    digest_obj,
    exclusive_directory_publish,
    load_json,
    temporary_directory,
)


class CheckpointStore:
    def __init__(self, run_dir: Path) -> None:
        self.run_dir = run_dir
        self.directory = run_dir / "checkpoints"
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock_path = run_dir / ".checkpoint.lock"

    def _record_paths(self) -> list[Path]:
        return sorted(self.directory.glob("[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9].json"))

    def records(self) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        expected = 1
        for path in self._record_paths():
            record = load_json(path)
            if int(record.get("sequence", -1)) != expected:
                raise VerifyFailed(
                    "checkpoint sequence is not contiguous",
                    path=str(path),
                    expected=expected,
                    actual=record.get("sequence"),
                )
            records.append(record)
            expected += 1
        return records

    def next_sequence(self) -> int:
        return len(self._record_paths()) + 1

    def write(
        self,
        *,
        stage: str,
        layer: str,
        unit: str,
        input_fingerprint: str,
        config_version: str,
        artifact_digest: str,
        stage_fingerprint: str,
    ) -> dict[str, Any]:
        with FileLock(self.lock_path):
            sequence = self.next_sequence()
            record = {
                "schema_version": SCHEMA_VERSION,
                "sequence": sequence,
                "stage": stage,
                "layer": layer,
                "unit": unit,
                "input_content_fingerprint": input_fingerprint,
                "config_version": config_version,
                "artifact_digest": artifact_digest,
                "stage_fingerprint": stage_fingerprint,
            }
            path = self.directory / f"{sequence:08d}.json"
            atomic_write_json(path, record)
            return record

    def find(
        self,
        *,
        stage: str,
        layer: str,
        unit: str,
        artifact_digest: str,
    ) -> dict[str, Any] | None:
        for record in self.records():
            if (
                record.get("stage") == stage
                and record.get("layer") == layer
                and record.get("unit") == unit
                and record.get("artifact_digest") == artifact_digest
            ):
                return record
        return None


def _stage_index(stage: str) -> int:
    try:
        return STAGES.index(stage)
    except ValueError as exc:
        raise InvalidStage("unknown pipeline stage", details={"stage": stage}) from exc


def cache_directory(root: Path, stage: str, fingerprint: str) -> Path:
    return root / "cache" / STAGE_LABELS[stage] / fingerprint


def load_cached_stage(
    root: Path, stage: str, fingerprint: str
) -> dict[str, Any] | None:
    directory = cache_directory(root, stage, fingerprint)
    result_path = directory / "result.json"
    manifest_path = directory / "manifest.json"
    if not result_path.is_file() or not manifest_path.is_file():
        return None
    manifest = load_json(manifest_path)
    payload = load_json(result_path)
    digest = digest_obj(payload)
    if manifest.get("stage") != stage or manifest.get("fingerprint") != fingerprint:
        raise StaleCheckpoint(
            "cached stage identity does not match its directory",
            details={"stage": stage, "fingerprint": fingerprint},
        )
    if manifest.get("artifact_sha256") != digest:
        raise VerifyFailed(
            "cached stage artifact digest mismatch",
            stage=stage,
            fingerprint=fingerprint,
        )
    return {
        "payload": payload,
        "manifest": manifest,
        "directory": str(directory),
        "artifact_sha256": digest,
    }


def stage_cache_available(root: Path, stage: str, fingerprint: str) -> bool:
    directory = cache_directory(root, stage, fingerprint)
    return (directory / "result.json").is_file() and (directory / "manifest.json").is_file()


def _publish_cache_locked(
    *,
    root: Path,
    stage: str,
    fingerprint: str,
    payload: dict[str, Any],
    config_id: str,
    input_fingerprint: str,
) -> dict[str, Any]:
    final_dir = cache_directory(root, stage, fingerprint)
    artifact_digest = digest_obj(payload)
    layer_digests = {
        layer_id: digest_obj(layer_payload)
        for layer_id, layer_payload in payload.get("layers", {}).items()
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "stage": stage,
        "fingerprint": fingerprint,
        "input_fingerprint": input_fingerprint,
        "config_id": config_id,
        "artifact_sha256": artifact_digest,
        "layer_sha256": layer_digests,
    }
    if final_dir.exists():
        existing = load_cached_stage(root, stage, fingerprint)
        if existing is None or existing["artifact_sha256"] != artifact_digest:
            raise StaleCheckpoint(
                "refusing to overwrite an immutable stage publication",
                details={"stage": stage, "fingerprint": fingerprint},
            )
        return existing
    with temporary_directory(final_dir.parent, f".tmp-{stage}-") as temp_dir:
        atomic_write_json(temp_dir / "result.json", payload)
        atomic_write_json(temp_dir / "manifest.json", manifest, pretty=True)
        if exclusive_directory_publish(temp_dir, final_dir):
            return {
                "payload": payload,
                "manifest": manifest,
                "directory": str(final_dir),
                "artifact_sha256": artifact_digest,
            }
    existing = load_cached_stage(root, stage, fingerprint)
    if existing is None or existing["artifact_sha256"] != artifact_digest:
        raise StaleCheckpoint(
            "concurrent stage publication conflicts with existing artifact",
            details={"stage": stage, "fingerprint": fingerprint},
        )
    return existing


class StagePublisher:
    """Sequential, file-locked publisher for immutable stage artifacts."""

    def __init__(
        self,
        *,
        root: Path,
        run_dir: Path,
        run_id: str,
        config_id: str,
        config_version: str,
        fingerprints: dict[str, str],
        layer_ids: list[str],
    ) -> None:
        self.root = root
        self.run_dir = run_dir
        self.run_id = run_id
        self.config_id = config_id
        self.config_version = config_version
        self.fingerprints = fingerprints
        self.layer_ids = layer_ids
        self.ledger_path = run_dir / "publish_ledger.json"
        self.lock_path = root / "locks" / "publication.lock"
        self.layer_dir = run_dir / "stage_layers"
        self.checkpoints = CheckpointStore(run_dir)

    def _new_ledger(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "run_id": self.run_id,
            "config_id": self.config_id,
            "config_version": self.config_version,
            "stage_fingerprints": dict(self.fingerprints),
            "layer_ids": list(self.layer_ids),
            "stages": {
                stage: {
                    "fingerprint": self.fingerprints[stage],
                    "status": "pending",
                    "next_layer_seq": 1,
                    "published_layers": [],
                    "stage_artifact_digest": None,
                    "cache_path": None,
                }
                for stage in STAGES
            },
        }

    def ensure_ledger(self) -> dict[str, Any]:
        with FileLock(self.lock_path):
            if self.ledger_path.exists():
                ledger = load_json(self.ledger_path)
                if (
                    ledger.get("config_id") != self.config_id
                    or ledger.get("stage_fingerprints") != self.fingerprints
                ):
                    raise StaleCheckpoint(
                        "run ledger belongs to a different configuration version",
                        details={
                            "run_id": self.run_id,
                            "use_new_run_id": True,
                            "config_id": ledger.get("config_id"),
                        },
                    )
                return ledger
            ledger = self._new_ledger()
            atomic_write_json(self.ledger_path, ledger)
            return ledger

    def load_ledger(self) -> dict[str, Any]:
        if not self.ledger_path.exists():
            return self.ensure_ledger()
        return load_json(self.ledger_path)

    def _check_prerequisite_locked(self, ledger: dict[str, Any], stage: str) -> None:
        index = _stage_index(stage)
        if index == 0:
            return
        prior = STAGES[index - 1]
        if ledger["stages"][prior]["status"] != "complete":
            raise InvalidStage(
                "preceding stage has not been published",
                details={"stage": stage, "required_stage": prior},
            )

    def _layer_payload_path(self, stage: str, sequence: int) -> Path:
        return self.layer_dir / stage / f"{sequence:08d}.json"

    def publish_layer(
        self,
        *,
        stage: str,
        layer_seq: int,
        layer_id: str,
        input_fingerprint: str,
        artifact_digest: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if stage not in STAGES:
            raise InvalidStage("unknown pipeline stage", details={"stage": stage})
        with FileLock(self.lock_path):
            ledger = self.load_ledger()
            stage_entry = ledger["stages"][stage]
            self._check_prerequisite_locked(ledger, stage)
            if stage_entry["status"] == "complete":
                raise StaleCheckpoint(
                    "stage is already complete; layer publication is stale",
                    details={"stage": stage, "layer_seq": layer_seq},
                )
            expected_seq = int(stage_entry["next_layer_seq"])
            if int(layer_seq) != expected_seq:
                raise StaleCheckpoint(
                    "layer sequence must advance contiguously",
                    layer=layer_id,
                    details={
                        "stage": stage,
                        "expected_layer_seq": expected_seq,
                        "received_layer_seq": int(layer_seq),
                    },
                )
            if int(layer_seq) > len(self.layer_ids):
                raise StaleCheckpoint(
                    "layer sequence exceeds stage layer count",
                    layer=layer_id,
                    details={"stage": stage, "layer_count": len(self.layer_ids)},
                )
            expected_layer = self.layer_ids[int(layer_seq) - 1]
            if layer_id != expected_layer:
                raise StaleCheckpoint(
                    "cross-layer publication is not allowed",
                    layer=layer_id,
                    details={
                        "stage": stage,
                        "expected_layer": expected_layer,
                        "received_layer": layer_id,
                    },
                )
            if stage_entry.get("fingerprint") != self.fingerprints[stage]:
                raise StaleCheckpoint(
                    "stage fingerprint changed inside an active run",
                    details={"stage": stage},
                )
            payload_path = self._layer_payload_path(stage, int(layer_seq))
            relative_payload = (
                payload_path.relative_to(self.run_dir).as_posix()
                if payload is not None
                else None
            )
            if payload is not None:
                actual_digest = digest_obj(payload)
                if actual_digest != artifact_digest:
                    raise StaleCheckpoint(
                        "provided artifact digest does not match layer payload",
                        layer=layer_id,
                        details={"stage": stage, "layer_seq": layer_seq},
                    )
                atomic_write_json(payload_path, payload)
                if payload_path.exists():
                    stored = load_json(payload_path)
                    if digest_obj(stored) != artifact_digest:
                        raise StaleCheckpoint(
                            "published layer artifact failed verification",
                            layer=layer_id,
                        )
            record = {
                "layer_seq": int(layer_seq),
                "layer": layer_id,
                "input_fingerprint": input_fingerprint,
                "artifact_digest": artifact_digest,
                "payload_file": relative_payload,
            }
            stage_entry["published_layers"].append(record)
            stage_entry["next_layer_seq"] = int(layer_seq) + 1
            stage_entry["status"] = "in_progress"
            atomic_write_json(self.ledger_path, ledger)
            self.checkpoints.write(
                stage=stage,
                layer=layer_id,
                unit="layer",
                input_fingerprint=input_fingerprint,
                config_version=self.config_version,
                artifact_digest=artifact_digest,
                stage_fingerprint=self.fingerprints[stage],
            )
            return record

    def complete_stage(
        self,
        *,
        stage: str,
        payload: dict[str, Any],
        input_fingerprint: str,
    ) -> dict[str, Any]:
        if stage not in STAGES:
            raise InvalidStage("unknown pipeline stage", details={"stage": stage})
        with FileLock(self.lock_path):
            ledger = self.load_ledger()
            self._check_prerequisite_locked(ledger, stage)
            entry = ledger["stages"][stage]
            if entry["status"] == "complete":
                cached = load_cached_stage(
                    self.root, stage, self.fingerprints[stage]
                )
                if cached is None or cached["artifact_sha256"] != entry.get(
                    "stage_artifact_digest"
                ):
                    raise StaleCheckpoint(
                        "completed stage artifact is missing or changed",
                        details={"stage": stage},
                    )
                return entry
            expected_next = len(self.layer_ids) + 1
            if int(entry["next_layer_seq"]) != expected_next:
                raise InvalidStage(
                    "cannot complete a stage before all layers are published",
                    details={
                        "stage": stage,
                        "next_layer_seq": entry["next_layer_seq"],
                        "expected_layer_seq": expected_next,
                    },
                )
            for index, layer_id in enumerate(self.layer_ids, start=1):
                record = entry["published_layers"][index - 1]
                if record.get("layer_seq") != index or record.get("layer") != layer_id:
                    raise StaleCheckpoint(
                        "published layer ledger is not contiguous",
                        layer=layer_id,
                        details={"stage": stage, "expected_layer_seq": index},
                    )
            cached = _publish_cache_locked(
                root=self.root,
                stage=stage,
                fingerprint=self.fingerprints[stage],
                payload=payload,
                config_id=self.config_id,
                input_fingerprint=input_fingerprint,
            )
            artifact_digest = cached["artifact_sha256"]
            entry["status"] = "complete"
            entry["stage_artifact_digest"] = artifact_digest
            entry["cache_path"] = cached["directory"]
            atomic_write_json(self.ledger_path, ledger)
            self.checkpoints.write(
                stage=stage,
                layer="*",
                unit="stage_complete",
                input_fingerprint=input_fingerprint,
                config_version=self.config_version,
                artifact_digest=artifact_digest,
                stage_fingerprint=self.fingerprints[stage],
            )
            return entry

    def load_published_layer(self, stage: str, layer_seq: int) -> dict[str, Any]:
        ledger = self.load_ledger()
        stage_entry = ledger["stages"][stage]
        if layer_seq < 1 or layer_seq > len(stage_entry["published_layers"]):
            raise InvalidStage(
                "requested layer was not published",
                details={"stage": stage, "layer_seq": layer_seq},
            )
        record = stage_entry["published_layers"][layer_seq - 1]
        relative = record.get("payload_file")
        if not relative:
            raise InvalidStage(
                "published layer has no stored payload",
                details={"stage": stage, "layer_seq": layer_seq},
            )
        payload = load_json(self.run_dir / relative)
        if digest_obj(payload) != record.get("artifact_digest"):
            raise VerifyFailed(
                "published layer payload digest mismatch",
                stage=stage,
                layer_seq=layer_seq,
            )
        return payload

    def repair_checkpoints_from_ledger(self) -> None:
        ledger = self.load_ledger()
        existing = self.checkpoints.records()
        for stage in STAGES:
            entry = ledger["stages"][stage]
            for record in entry.get("published_layers", []):
                matches = [
                    checkpoint
                    for checkpoint in existing
                    if checkpoint.get("stage") == stage
                    and checkpoint.get("layer") == record["layer"]
                    and checkpoint.get("unit") == "layer"
                    and checkpoint.get("artifact_digest") == record["artifact_digest"]
                ]
                if not matches:
                    self.checkpoints.write(
                        stage=stage,
                        layer=record["layer"],
                        unit="layer",
                        input_fingerprint=record["input_fingerprint"],
                        config_version=self.config_version,
                        artifact_digest=record["artifact_digest"],
                        stage_fingerprint=self.fingerprints[stage],
                    )
            if entry.get("status") == "complete":
                digest = entry.get("stage_artifact_digest")
                matches = [
                    checkpoint
                    for checkpoint in existing
                    if checkpoint.get("stage") == stage
                    and checkpoint.get("unit") == "stage_complete"
                    and checkpoint.get("artifact_digest") == digest
                ]
                if not matches:
                    self.checkpoints.write(
                        stage=stage,
                        layer="*",
                        unit="stage_complete",
                        input_fingerprint=f"stage:{entry['fingerprint']}",
                        config_version=self.config_version,
                        artifact_digest=digest,
                        stage_fingerprint=self.fingerprints[stage],
                    )


def stop_request_path(root: Path) -> Path:
    return root / "control" / "stop.request"


def request_stop(root: Path, *, reason: str = "client_request") -> None:
    atomic_write_json(
        stop_request_path(root),
        {"schema_version": SCHEMA_VERSION, "reason": reason},
    )


def stop_requested(root: Path) -> bool:
    return stop_request_path(root).is_file()


def clear_stop(root: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        stop_request_path(root).unlink()
    if os.name != "nt":
        with contextlib.suppress(OSError):
            descriptor = os.open(root / "control", os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
