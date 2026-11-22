"""Public workspace, query, and import operations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .checkpoints import STAGES, CheckpointStore, safe_shard_id
from .config import ShardConfig, load_manifest, load_reference_pairs
from .errors import E_INVALID_STAGE, E_STALE_CHECKPOINT, FingerprintError
from .jsonutil import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    read_json,
    sha256_bytes,
    sha256_file,
)
from .pipeline import (
    ArtifactLayout,
    _stage_version_audit,
    _stage_version_pairs,
    _stage_version_peaks,
    _stage_version_quantize,
)
from .wavio import read_wav


def _workspace_manifest_path(workspace: Path, manifest_path: str | None) -> Path | None:
    if manifest_path is not None:
        return Path(manifest_path).resolve()
    saved = workspace / "manifest.json"
    return saved if saved.exists() else None


def _checkpoint_summary(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if record is None:
        return None
    return {
        "seq": record.get("seq"),
        "generation": record.get("generation"),
        "stage_version": record.get("stage_version"),
        "config_version": record.get("config_version"),
        "input_digest": record.get("input_digest"),
        "artifact_digest": record.get("artifact_digest"),
    }


def _expected_versions(
    layout: ArtifactLayout,
    config: ShardConfig,
    state: dict[str, Any],
    sample_digest: str,
    reference_digest: str,
) -> dict[str, str | None]:
    expected: dict[str, str | None] = {
        "peaks": _stage_version_peaks(config, sample_digest),
        "pairs": None,
        "quantize": None,
        "audit": None,
    }
    peak_record = state.get("stages", {}).get("peaks")
    if peak_record is not None and peak_record.get("stage_version") == expected["peaks"]:
        peaks_digest = str(peak_record["artifact_digest"])
        expected["pairs"] = _stage_version_pairs(config, str(expected["peaks"]), peaks_digest)
        pair_record = state.get("stages", {}).get("pairs")
        if pair_record is not None and pair_record.get("stage_version") == expected["pairs"]:
            pairs_digest = str(pair_record["artifact_digest"])
            expected["quantize"] = _stage_version_quantize(config, str(expected["pairs"]), pairs_digest)
            quantized_record = state.get("stages", {}).get("quantize")
            if (
                quantized_record is not None
                and quantized_record.get("stage_version") == expected["quantize"]
            ):
                quantized_digest = str(quantized_record["artifact_digest"])
                expected["audit"] = _stage_version_audit(
                    config, str(expected["quantize"]), quantized_digest, reference_digest
                )
    return expected


def _discover_shards(workspace: Path) -> list[str]:
    identifiers: set[str] = set()
    root = workspace / "checkpoints"
    if root.exists():
        for state_path in root.glob("*/state.json"):
            try:
                payload = read_json(state_path)
            except (OSError, json.JSONDecodeError):
                continue
            shard_id = payload.get("shard_id")
            if isinstance(shard_id, str):
                identifiers.add(shard_id)
    errors_root = workspace / "errors"
    if errors_root.exists():
        for error_path in errors_root.glob("*.json"):
            try:
                payload = read_json(error_path)
            except (OSError, json.JSONDecodeError):
                continue
            shard_id = payload.get("shard_id")
            if isinstance(shard_id, str):
                identifiers.add(shard_id)
    return sorted(identifiers)


def query_status(
    workspace: str | Path,
    manifest_path: str | None = None,
    shard_id: str | None = None,
) -> dict[str, Any]:
    workspace_path = Path(workspace).resolve()
    layout = ArtifactLayout(workspace_path)
    store = CheckpointStore(workspace_path)
    resolved_manifest = _workspace_manifest_path(workspace_path, manifest_path)
    manifest = load_manifest(resolved_manifest) if resolved_manifest is not None else None
    configs = manifest.by_id() if manifest is not None else {}
    identifiers = sorted(configs) if configs else _discover_shards(workspace_path)
    if shard_id is not None:
        identifiers = [identifier for identifier in identifiers if identifier == shard_id]

    rows: list[dict[str, Any]] = []
    for identifier in identifiers:
        state = store.read_state(identifier)
        config = configs.get(identifier)
        sample_digest = None
        reference_digest = None
        config_version = None
        expected: dict[str, str | None] = {stage: None for stage in STAGES}
        if config is not None:
            if Path(config.wav_path).exists():
                sample_digest = sha256_file(config.wav_path)
            if Path(config.reference_path).exists():
                reference_digest = sha256_file(config.reference_path)
            if reference_digest is not None:
                config_version = config.config_version(reference_digest)
                if sample_digest is not None:
                    expected = _expected_versions(
                        layout, config, state, sample_digest, reference_digest
                    )
        checkpoints = {
            stage: _checkpoint_summary(state.get("stages", {}).get(stage)) for stage in STAGES
        }
        stale_stages = []
        if config is not None:
            for stage in STAGES:
                record = state.get("stages", {}).get(stage)
                expected_version = expected[stage]
                if expected_version is not None and (
                    record is None or record.get("stage_version") != expected_version
                ):
                    stale_stages.append(stage)
        error_path = layout.error_path(identifier)
        last_error = read_json(error_path) if error_path.exists() else None
        rows.append(
            {
                "shard_id": identifier,
                "generation": state.get("generation"),
                "next_seq": state.get("next_seq"),
                "checkpoints": checkpoints,
                "last_invalidation": state.get("last_invalidation"),
                "sample_digest": sample_digest,
                "reference_digest": reference_digest,
                "config_version": config_version,
                "expected_stage_versions": expected,
                "stale_stages": stale_stages,
                "last_error": last_error,
            }
        )
    return {
        "schema": "audiofp.status.v1",
        "manifest": str(resolved_manifest) if resolved_manifest is not None else None,
        "shard_count": len(rows),
        "shards": rows,
    }


def load_stage_artifact(
    workspace: str | Path, shard_id: str, stage: str
) -> dict[str, Any]:
    if stage not in STAGES:
        raise FingerprintError(
            E_INVALID_STAGE,
            "unknown stage requested",
            shard_id=shard_id,
            stage=stage,
            details={"allowed_stages": list(STAGES)},
        )
    workspace_path = Path(workspace).resolve()
    layout = ArtifactLayout(workspace_path)
    store = CheckpointStore(workspace_path)
    state = store.read_state(shard_id)
    record = state.get("stages", {}).get(stage)
    if record is None:
        raise FingerprintError(
            E_INVALID_STAGE,
            "requested stage has no completed checkpoint",
            shard_id=shard_id,
            stage=stage,
            details={"reason": "missing_predecessor_or_stage"},
        )
    path = layout.stage_path(shard_id, stage)
    if not path.exists():
        raise FingerprintError(
            E_STALE_CHECKPOINT,
            "stage checkpoint artifact is missing",
            shard_id=shard_id,
            stage=stage,
            details={"reason": "missing_artifact"},
        )
    if sha256_file(path) != record.get("artifact_digest"):
        raise FingerprintError(
            E_STALE_CHECKPOINT,
            "stage artifact no longer matches its checkpoint digest",
            shard_id=shard_id,
            stage=stage,
            details={"reason": "artifact_digest_mismatch"},
        )
    return read_json(path)


def get_report(workspace: str | Path) -> dict[str, Any]:
    layout = ArtifactLayout(Path(workspace).resolve())
    summary_path = layout.summary_path()
    digest_path = layout.summary_digest_path()
    if not summary_path.exists() or not digest_path.exists():
        raise FingerprintError(
            E_INVALID_STAGE,
            "a complete audit report is not available",
            stage="audit",
            details={"reason": "incomplete_run"},
        )
    summary = read_json(summary_path)
    summary_digest = digest_path.read_text(encoding="ascii").strip()
    actual_digest = sha256_file(summary_path)
    if actual_digest != summary_digest:
        raise FingerprintError(
            E_STALE_CHECKPOINT,
            "audit summary digest does not match its sidecar",
            stage="audit",
            details={"reason": "summary_digest_mismatch"},
        )
    return {"summary_digest": summary_digest, "summary": summary}


def import_dataset(workspace: str | Path, manifest_path: str | Path) -> dict[str, Any]:
    workspace_path = Path(workspace).resolve()
    layout = ArtifactLayout(workspace_path)
    layout.init()
    manifest = load_manifest(manifest_path)
    input_root = workspace_path / "input"
    wav_dir = input_root / "wav"
    reference_dir = input_root / "references"
    wav_dir.mkdir(parents=True, exist_ok=True)
    reference_dir.mkdir(parents=True, exist_ok=True)

    imported: list[dict[str, Any]] = []
    reference_copies: dict[str, str] = {}
    for config in manifest.shards:
        read_wav(config.wav_path, config.shard_id)
        load_reference_pairs(config.reference_path, config.shard_id)
        wav_source = Path(config.wav_path)
        wav_destination = wav_dir / f"{safe_shard_id(config.shard_id)}-{wav_source.name}"
        atomic_write_bytes(wav_destination, wav_source.read_bytes())
        reference_source = Path(config.reference_path)
        reference_key = sha256_file(reference_source)
        if reference_key not in reference_copies:
            reference_destination = reference_dir / f"references-{reference_key[:12]}.json"
            atomic_write_bytes(reference_destination, reference_source.read_bytes())
            reference_copies[reference_key] = reference_destination.relative_to(workspace_path).as_posix()
        wav_relative = wav_destination.relative_to(workspace_path).as_posix()
        shard_payload = config.auditable_dict()
        shard_payload["wav"] = wav_relative
        shard_payload["reference"] = reference_copies[reference_key]
        imported.append(shard_payload)

    imported_manifest = {
        "schema": manifest.schema,
        "seed": manifest.seed,
        "shards": sorted(imported, key=lambda item: item["shard_id"]),
    }
    saved_path = layout.manifest_path()
    atomic_write_json(saved_path, imported_manifest)
    return {
        "schema": "audiofp.import-result.v1",
        "manifest": str(saved_path),
        "shard_count": len(imported),
        "shard_ids": [item["shard_id"] for item in imported],
        "reference_file_count": len(reference_copies),
    }


def canonical_status_bytes(payload: dict[str, Any]) -> bytes:
    return canonical_json_bytes(payload)

