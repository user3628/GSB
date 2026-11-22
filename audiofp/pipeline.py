"""Four-stage resumable audit pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .audit import audit_shard, build_summary
from .checkpoints import STAGES, STAGE_INDEX, CheckpointStore, safe_shard_id
from .config import (
    Manifest,
    ReferencePair,
    ShardConfig,
    load_manifest,
    load_reference_pairs,
)
from .dsp import Peak, PeakResult, detect_peaks, peaks_artifact
from .errors import E_INVALID_CONFIG, E_INVALID_STAGE, FingerprintError
from .jsonutil import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    object_digest,
    read_json,
    sha256_bytes,
    sha256_file,
    version_fingerprint,
)
from .locking import FileLock
from .pairing import FingerprintPair, generate_fingerprint_pairs, pairs_artifact
from .quantize import quantization_artifact, quantize_pairs
from .wavio import read_wav


WORKSPACE_SCHEMA = "audiofp.workspace.v1"
STOP_SCHEMA = "audiofp.stop.v1"


@dataclass(frozen=True)
class ArtifactLayout:
    workspace: Path

    def init(self) -> None:
        for relative in (
            "artifacts/peaks",
            "artifacts/pairs",
            "artifacts/quantized",
            "reports/shards",
            "errors",
            "checkpoints",
            ".locks",
        ):
            (self.workspace / relative).mkdir(parents=True, exist_ok=True)
        workspace_record = {
            "schema": WORKSPACE_SCHEMA,
            "layout_version": 1,
            "checkpoint_schema": "audiofp.checkpoint.v1",
            "report_schema": "audiofp.summary.v1",
        }
        atomic_write_json(self.workspace / "workspace.json", workspace_record)
        probe = self.workspace / ".locks" / ".write-probe"
        atomic_write_bytes(probe, b"ok\n")
        probe.unlink(missing_ok=True)

    def peaks_path(self, shard_id: str) -> Path:
        return self.workspace / "artifacts" / "peaks" / f"{safe_shard_id(shard_id)}.json"

    def pairs_path(self, shard_id: str) -> Path:
        return self.workspace / "artifacts" / "pairs" / f"{safe_shard_id(shard_id)}.json"

    def quantized_path(self, shard_id: str) -> Path:
        return self.workspace / "artifacts" / "quantized" / f"{safe_shard_id(shard_id)}.json"

    def audit_path(self, shard_id: str) -> Path:
        return self.workspace / "reports" / "shards" / f"{safe_shard_id(shard_id)}.json"

    def error_path(self, shard_id: str) -> Path:
        return self.workspace / "errors" / f"{safe_shard_id(shard_id)}.json"

    def stage_path(self, shard_id: str, stage: str) -> Path:
        return {
            "peaks": self.peaks_path,
            "pairs": self.pairs_path,
            "quantize": self.quantized_path,
            "audit": self.audit_path,
        }[stage](shard_id)

    def summary_path(self) -> Path:
        return self.workspace / "reports" / "summary.json"

    def summary_digest_path(self) -> Path:
        return self.workspace / "reports" / "summary.sha256"

    def stop_path(self) -> Path:
        return self.workspace / ".stop_requested"

    def manifest_path(self) -> Path:
        return self.workspace / "manifest.json"

    def remove_from(self, shard_id: str, stage: str) -> None:
        for name in STAGES[STAGE_INDEX[stage] :]:
            self.stage_path(shard_id, name).unlink(missing_ok=True)

    def remove_summary(self) -> None:
        self.summary_path().unlink(missing_ok=True)
        self.summary_digest_path().unlink(missing_ok=True)


@dataclass(frozen=True)
class RunResult:
    stopped: bool
    stop_reason: str | None
    processed_shards: int
    total_shards: int
    report_path: str | None
    summary_digest: str | None
    errors: tuple[dict[str, Any], ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "stopped": self.stopped,
            "stop_reason": self.stop_reason,
            "processed_shards": self.processed_shards,
            "total_shards": self.total_shards,
            "report_path": self.report_path,
            "summary_digest": self.summary_digest,
            "errors": list(self.errors),
        }


class _StageStop(Exception):
    def __init__(self, reason: str, shard_id: str, stage: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.shard_id = shard_id
        self.stage = stage


def _flatten(prefix: str, value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        flattened: dict[str, Any] = {}
        for key in sorted(value):
            flattened.update(_flatten(f"{prefix}.{key}" if prefix else str(key), value[key]))
        return flattened
    return {prefix: value}


def _changed_fields(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    old_flat = _flatten("", old)
    new_flat = _flatten("", new)
    fields = sorted(
        key
        for key in set(old_flat) | set(new_flat)
        if old_flat.get(key, object()) != new_flat.get(key, object())
    )
    return fields


def _artifact_intact(path: Path, record: dict[str, Any]) -> bool:
    if not path.exists():
        return False
    try:
        return sha256_file(path) == record.get("artifact_digest")
    except OSError:
        return False


def _write_artifact(path: Path, payload: dict[str, Any]) -> tuple[bytes, str]:
    data = atomic_write_json(path, payload)
    return data, sha256_bytes(data)


def _peak_result_from_artifact(payload: dict[str, Any]) -> PeakResult:
    peaks = tuple(
        Peak(frame=int(item["frame"]), freq=int(item["freq"]), amplitude=float(item["amplitude"]))
        for item in payload["peaks"]
    )
    return PeakResult(
        peaks=peaks,
        n_frames=int(payload["n_frames"]),
        n_freq_bins=int(payload["n_freq_bins"]),
        sample_count=int(payload["sample_count"]),
        frame_length=int(payload["frame_length"]),
        hop_length=int(payload["hop_length"]),
    )


def _pairs_from_artifact(payload: dict[str, Any]) -> tuple[FingerprintPair, ...]:
    return tuple(
        FingerprintPair(
            pair_index=int(item["pair_index"]),
            anchor_frame=int(item["anchor_frame"]),
            anchor_freq=int(item["anchor_freq"]),
            target_frame=int(item["target_frame"]),
            target_freq=int(item["target_freq"]),
            dt=int(item["dt"]),
            anchor_amplitude=float(item["anchor_amplitude"]),
            target_amplitude=float(item["target_amplitude"]),
        )
        for item in payload["pairs"]
    )


def _stage_version_peaks(config: ShardConfig, sample_digest: str) -> str:
    return version_fingerprint("audiofp.stage.peaks.v1", config.peak_params(), sample_digest)


def _stage_version_pairs(config: ShardConfig, peaks_version: str, peaks_digest: str) -> str:
    return version_fingerprint(
        "audiofp.stage.pairs.v1", peaks_version, peaks_digest, config.pair_params()
    )


def _stage_version_quantize(config: ShardConfig, pairs_version: str, pairs_digest: str) -> str:
    return version_fingerprint(
        "audiofp.stage.quantize.v1", pairs_version, pairs_digest, config.quant_params()
    )


def _stage_version_audit(
    config: ShardConfig, quantized_version: str, quantized_digest: str, reference_digest: str
) -> str:
    return version_fingerprint(
        "audiofp.stage.audit.v1",
        quantized_version,
        quantized_digest,
        reference_digest,
        config.audit_params(),
    )


def _find_invalidation(
    layout: ArtifactLayout,
    state: dict[str, Any],
    config: ShardConfig,
    sample_digest: str,
    reference_digest: str,
) -> dict[str, Any] | None:
    """Find the earliest stage whose stored dependency fingerprint is stale."""

    stages = state.get("stages", {})
    if not stages:
        return None

    peaks_version = _stage_version_peaks(config, sample_digest)
    peak_deps = {"sample_digest": sample_digest, "params": config.peak_params()}
    peak_record = stages.get("peaks")
    if peak_record is None:
        if any(name in stages for name in STAGES[1:]):
            return {
                "stage": "peaks",
                "reason": "missing_checkpoint",
                "changed_fields": ["peaks"],
                "old_versions": {},
                "new_versions": {"peaks": peaks_version},
            }
        return None
    if peak_record.get("stage_version") != peaks_version or peak_record.get("dependencies") != peak_deps:
        changed = _changed_fields(peak_record.get("dependencies", {}), peak_deps)
        return {
            "stage": "peaks",
            "reason": changed[0] if changed else "stage_algorithm_version",
            "changed_fields": changed,
            "old_versions": {"peaks": peak_record.get("stage_version")},
            "new_versions": {"peaks": peaks_version},
        }
    if not _artifact_intact(layout.peaks_path(config.shard_id), peak_record):
        return {
            "stage": "peaks",
            "reason": "artifact_digest_mismatch",
            "changed_fields": ["artifact"],
            "old_versions": {"peaks": peak_record.get("stage_version")},
            "new_versions": {"peaks": peaks_version},
        }

    peaks_digest = str(peak_record["artifact_digest"])
    pairs_version = _stage_version_pairs(config, peaks_version, peaks_digest)
    pair_deps = {
        "peaks_version": peaks_version,
        "peaks_digest": peaks_digest,
        "params": config.pair_params(),
    }
    pair_record = stages.get("pairs")
    if pair_record is None:
        if any(name in stages for name in STAGES[2:]):
            return {
                "stage": "pairs",
                "reason": "missing_checkpoint",
                "changed_fields": ["pairs"],
                "old_versions": {},
                "new_versions": {"pairs": pairs_version},
            }
        return None
    if pair_record.get("stage_version") != pairs_version or pair_record.get("dependencies") != pair_deps:
        changed = _changed_fields(pair_record.get("dependencies", {}), pair_deps)
        return {
            "stage": "pairs",
            "reason": changed[0] if changed else "stage_algorithm_version",
            "changed_fields": changed,
            "old_versions": {"pairs": pair_record.get("stage_version")},
            "new_versions": {"pairs": pairs_version},
        }
    if not _artifact_intact(layout.pairs_path(config.shard_id), pair_record):
        return {
            "stage": "pairs",
            "reason": "artifact_digest_mismatch",
            "changed_fields": ["artifact"],
            "old_versions": {"pairs": pair_record.get("stage_version")},
            "new_versions": {"pairs": pairs_version},
        }

    pairs_digest = str(pair_record["artifact_digest"])
    quantized_version = _stage_version_quantize(config, pairs_version, pairs_digest)
    quantized_deps = {
        "pairs_version": pairs_version,
        "pairs_digest": pairs_digest,
        "params": config.quant_params(),
    }
    quantized_record = stages.get("quantize")
    if quantized_record is None:
        if "audit" in stages:
            return {
                "stage": "quantize",
                "reason": "missing_checkpoint",
                "changed_fields": ["quantize"],
                "old_versions": {},
                "new_versions": {"quantize": quantized_version},
            }
        return None
    if (
        quantized_record.get("stage_version") != quantized_version
        or quantized_record.get("dependencies") != quantized_deps
    ):
        changed = _changed_fields(quantized_record.get("dependencies", {}), quantized_deps)
        return {
            "stage": "quantize",
            "reason": changed[0] if changed else "stage_algorithm_version",
            "changed_fields": changed,
            "old_versions": {"quantize": quantized_record.get("stage_version")},
            "new_versions": {"quantize": quantized_version},
        }
    if not _artifact_intact(layout.quantized_path(config.shard_id), quantized_record):
        return {
            "stage": "quantize",
            "reason": "artifact_digest_mismatch",
            "changed_fields": ["artifact"],
            "old_versions": {"quantize": quantized_record.get("stage_version")},
            "new_versions": {"quantize": quantized_version},
        }

    quantized_digest = str(quantized_record["artifact_digest"])
    audit_version = _stage_version_audit(config, quantized_version, quantized_digest, reference_digest)
    audit_deps = {
        "quantize_version": quantized_version,
        "quantize_digest": quantized_digest,
        "reference_digest": reference_digest,
        "params": config.audit_params(),
    }
    audit_record = stages.get("audit")
    if audit_record is None:
        return None
    if audit_record.get("stage_version") != audit_version or audit_record.get("dependencies") != audit_deps:
        changed = _changed_fields(audit_record.get("dependencies", {}), audit_deps)
        return {
            "stage": "audit",
            "reason": changed[0] if changed else "stage_algorithm_version",
            "changed_fields": changed,
            "old_versions": {"audit": audit_record.get("stage_version")},
            "new_versions": {"audit": audit_version},
        }
    if not _artifact_intact(layout.audit_path(config.shard_id), audit_record):
        return {
            "stage": "audit",
            "reason": "artifact_digest_mismatch",
            "changed_fields": ["artifact"],
            "old_versions": {"audit": audit_record.get("stage_version")},
            "new_versions": {"audit": audit_version},
        }
    return None


def _publish(
    store: CheckpointStore,
    shard_id: str,
    stage: str,
    artifact_path: Path,
    artifact_digest: str,
    input_digest: str,
    config_version: str,
    stage_version: str,
    dependencies: dict[str, Any],
    params: dict[str, Any],
) -> dict[str, Any]:
    state = store.read_state(shard_id)
    expected_seq = int(state["next_seq"])
    record = {
        "schema": "audiofp.checkpoint.v1",
        "shard_id": shard_id,
        "stage": stage,
        "input_digest": input_digest,
        "config_version": config_version,
        "artifact_digest": artifact_digest,
        "stage_version": stage_version,
        "dependencies": dependencies,
        "params": params,
    }
    return store.publish(shard_id, record, expected_seq=expected_seq, artifact_path=artifact_path)


def _checkpoint_stop(layout: ArtifactLayout, shard_id: str, stage: str, stop_after_stage: str | None) -> None:
    if layout.stop_path().exists():
        raise _StageStop("stop_requested", shard_id, stage)
    if stop_after_stage is not None and stage == stop_after_stage:
        raise _StageStop("stop_after_stage", shard_id, stage)


def _dataset_entry(config: ShardConfig) -> dict[str, Any]:
    sample_digest = sha256_file(config.wav_path) if Path(config.wav_path).exists() else None
    reference_digest = (
        sha256_file(config.reference_path) if Path(config.reference_path).exists() else None
    )
    return {
        "shard_id": config.shard_id,
        "sample_digest": sample_digest,
        "reference_digest": reference_digest,
        "config": config.auditable_dict(),
    }


def _write_error(
    layout: ArtifactLayout,
    store: CheckpointStore,
    config: ShardConfig,
    error: FingerprintError,
    config_version: str | None,
    sample_digest: str | None,
) -> dict[str, Any]:
    state = store.read_state(config.shard_id)
    payload = {
        "schema": "audiofp.error.v1",
        "shard_id": config.shard_id,
        "code": error.code,
        "message": error.message,
        "stage": error.stage,
        "details": error.details,
        "config_version": config_version,
        "sample_digest": sample_digest,
        "checkpoints": {
            stage: {
                "seq": record.get("seq"),
                "stage_version": record.get("stage_version"),
                "artifact_digest": record.get("artifact_digest"),
            }
            for stage, record in state.get("stages", {}).items()
        },
    }
    atomic_write_json(layout.error_path(config.shard_id), payload)
    return payload


def _process_shard(
    layout: ArtifactLayout,
    store: CheckpointStore,
    config: ShardConfig,
    stop_after_stage: str | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    wav = read_wav(config.wav_path, config.shard_id)
    sample_digest = wav.content_digest
    reference_pairs, reference_digest = load_reference_pairs(config.reference_path, config.shard_id)
    config_version = config.config_version(reference_digest)
    dataset = {
        "shard_id": config.shard_id,
        "sample_digest": sample_digest,
        "reference_digest": reference_digest,
        "config": config.auditable_dict(),
    }

    state = store.read_state(config.shard_id)
    invalidation = _find_invalidation(
        layout, state, config, sample_digest=sample_digest, reference_digest=reference_digest
    )
    if invalidation is not None:
        layout.remove_from(config.shard_id, invalidation["stage"])
        layout.error_path(config.shard_id).unlink(missing_ok=True)
        store.invalidate_from(
            config.shard_id,
            invalidation["stage"],
            invalidation["reason"],
            changed_fields=invalidation["changed_fields"],
            old_versions=invalidation["old_versions"],
            new_versions=invalidation["new_versions"],
        )
        layout.remove_summary()

    state = store.read_state(config.shard_id)
    peaks_version = _stage_version_peaks(config, sample_digest)
    peak_record = state["stages"].get("peaks")
    if peak_record is not None and peak_record.get("stage_version") == peaks_version:
        peak_payload = read_json(layout.peaks_path(config.shard_id))
        peak_result = _peak_result_from_artifact(peak_payload)
        peaks_digest = str(peak_record["artifact_digest"])
    else:
        peak_result = detect_peaks(wav, config)
        peak_payload = peaks_artifact(
            peak_result, config, sample_digest, peaks_version
        )
        _, peaks_digest = _write_artifact(layout.peaks_path(config.shard_id), peak_payload)
        # Downstream stages consume the canonical on-disk representation.  This
        # removes any difference between a fresh run and a resumed run.
        peak_payload = read_json(layout.peaks_path(config.shard_id))
        peak_result = _peak_result_from_artifact(peak_payload)
        peak_record = _publish(
            store,
            config.shard_id,
            "peaks",
            layout.peaks_path(config.shard_id),
            peaks_digest,
            sample_digest,
            config_version,
            peaks_version,
            {"sample_digest": sample_digest, "params": config.peak_params()},
            config.peak_params(),
        )
    _checkpoint_stop(layout, config.shard_id, "peaks", stop_after_stage)

    pairs_version = _stage_version_pairs(config, peaks_version, peaks_digest)
    pair_record = state["stages"].get("pairs")
    if pair_record is not None and pair_record.get("stage_version") == pairs_version:
        pairs_payload = read_json(layout.pairs_path(config.shard_id))
        pairs_digest = str(pair_record["artifact_digest"])
    else:
        pair_result = generate_fingerprint_pairs(peak_result.peaks, config)
        pairs_payload = pairs_artifact(
            pair_result, config, peaks_digest, pairs_version
        )
        _, pairs_digest = _write_artifact(layout.pairs_path(config.shard_id), pairs_payload)
        pairs_payload = read_json(layout.pairs_path(config.shard_id))
        pair_record = _publish(
            store,
            config.shard_id,
            "pairs",
            layout.pairs_path(config.shard_id),
            pairs_digest,
            peaks_digest,
            config_version,
            pairs_version,
            {
                "peaks_version": peaks_version,
                "peaks_digest": peaks_digest,
                "params": config.pair_params(),
            },
            config.pair_params(),
        )
    _checkpoint_stop(layout, config.shard_id, "pairs", stop_after_stage)

    quantized_version = _stage_version_quantize(config, pairs_version, pairs_digest)
    quantized_record = state["stages"].get("quantize")
    if quantized_record is not None and quantized_record.get("stage_version") == quantized_version:
        quantized_payload = read_json(layout.quantized_path(config.shard_id))
        quantized_digest = str(quantized_record["artifact_digest"])
    else:
        pair_objects = _pairs_from_artifact(pairs_payload)
        quantized_result = quantize_pairs(pair_objects, config)
        quantized_payload = quantization_artifact(
            quantized_result, config, pairs_digest, quantized_version
        )
        _, quantized_digest = _write_artifact(
            layout.quantized_path(config.shard_id), quantized_payload
        )
        quantized_payload = read_json(layout.quantized_path(config.shard_id))
        quantized_record = _publish(
            store,
            config.shard_id,
            "quantize",
            layout.quantized_path(config.shard_id),
            quantized_digest,
            pairs_digest,
            config_version,
            quantized_version,
            {
                "pairs_version": pairs_version,
                "pairs_digest": pairs_digest,
                "params": config.quant_params(),
            },
            config.quant_params(),
        )
    _checkpoint_stop(layout, config.shard_id, "quantize", stop_after_stage)

    audit_version = _stage_version_audit(
        config, quantized_version, quantized_digest, reference_digest
    )
    audit_record = state["stages"].get("audit")
    if audit_record is not None and audit_record.get("stage_version") == audit_version:
        audit_payload = read_json(layout.audit_path(config.shard_id))
    else:
        audit_payload = audit_shard(
            config=config,
            peak_count=len(peak_result.peaks),
            reference_pairs=reference_pairs,
            reference_digest=reference_digest,
            pairs_digest=pairs_digest,
            quantized=quantized_payload,
            quantized_digest=quantized_digest,
            stage_version=audit_version,
        )
        _, audit_digest = _write_artifact(layout.audit_path(config.shard_id), audit_payload)
        audit_payload = read_json(layout.audit_path(config.shard_id))
        audit_record = _publish(
            store,
            config.shard_id,
            "audit",
            layout.audit_path(config.shard_id),
            audit_digest,
            quantized_digest,
            config_version,
            audit_version,
            {
                "quantize_version": quantized_version,
                "quantize_digest": quantized_digest,
                "reference_digest": reference_digest,
                "params": config.audit_params(),
            },
            config.audit_params(),
        )
    layout.error_path(config.shard_id).unlink(missing_ok=True)
    return audit_payload, dataset


def _save_manifest_snapshot(layout: ArtifactLayout, manifest: Manifest) -> None:
    payload = {
        "schema": manifest.schema,
        "seed": manifest.seed,
        "shards": [shard.snapshot() for shard in manifest.shards],
    }
    atomic_write_json(layout.manifest_path(), payload)


def _shard_run_lock(layout: ArtifactLayout, shard_id: str) -> FileLock:
    return FileLock(layout.workspace / ".locks" / f"run-{safe_shard_id(shard_id)}.lock")


def _manifest_for_run(workspace: Path, manifest_path: str | None) -> Manifest:
    if manifest_path is None:
        saved = workspace / "manifest.json"
        if not saved.exists():
            raise FingerprintError(
                E_INVALID_CONFIG,
                "no manifest was supplied and the workspace has no manifest snapshot",
                stage="config",
            )
        manifest_path = str(saved)
    return load_manifest(manifest_path)


def request_stop(workspace: str | Path) -> None:
    layout = ArtifactLayout(Path(workspace))
    layout.init()
    atomic_write_json(layout.stop_path(), {"schema": STOP_SCHEMA, "requested": True})


def clear_stop(workspace: str | Path) -> None:
    ArtifactLayout(Path(workspace)).stop_path().unlink(missing_ok=True)


def stop_requested(workspace: str | Path) -> bool:
    return ArtifactLayout(Path(workspace)).stop_path().exists()


def run_pipeline(
    workspace: str | Path,
    manifest_path: str | None = None,
    *,
    stop_after_stage: str | None = None,
    stop_after_shards: int | None = None,
    clear_stop_request: bool = False,
) -> RunResult:
    if stop_after_stage is not None and stop_after_stage not in STAGES:
        raise FingerprintError(
            E_INVALID_STAGE,
            "stop_after_stage is not a valid pipeline stage",
            stage="config",
            details={"allowed_stages": list(STAGES)},
        )
    workspace_path = Path(workspace).resolve()
    layout = ArtifactLayout(workspace_path)
    layout.init()
    if clear_stop_request:
        clear_stop(workspace_path)
    if stop_requested(workspace_path):
        manifest = _manifest_for_run(workspace_path, manifest_path)
        return RunResult(True, "stop_requested", 0, len(manifest.shards), None, None, tuple())

    manifest = _manifest_for_run(workspace_path, manifest_path)
    _save_manifest_snapshot(layout, manifest)
    layout.remove_summary()
    store = CheckpointStore(workspace_path)

    audits: dict[str, dict[str, Any]] = {}
    errors: dict[str, dict[str, Any]] = {}
    dataset_entries: list[dict[str, Any]] = []
    processed = 0
    for config in manifest.shards:
        if stop_requested(workspace_path):
            return RunResult(
                True,
                "stop_requested",
                processed,
                len(manifest.shards),
                None,
                None,
                tuple(errors[key] for key in sorted(errors)),
            )
        try:
            with _shard_run_lock(layout, config.shard_id):
                audit_payload, dataset = _process_shard(
                    layout, store, config, stop_after_stage=stop_after_stage
                )
            audits[config.shard_id] = audit_payload
            dataset_entries.append(dataset)
            processed += 1
        except _StageStop as stop:
            return RunResult(
                True,
                stop.reason,
                processed,
                len(manifest.shards),
                None,
                None,
                tuple(errors[key] for key in sorted(errors)),
            )
        except FingerprintError as error:
            sample_digest = sha256_file(config.wav_path) if Path(config.wav_path).exists() else None
            reference_digest = (
                sha256_file(config.reference_path) if Path(config.reference_path).exists() else None
            )
            config_version = (
                config.config_version(reference_digest) if reference_digest is not None else None
            )
            state = store.read_state(config.shard_id)
            if error.stage in ("input", "config") and state.get("stages"):
                layout.remove_from(config.shard_id, "peaks")
                store.invalidate_from(
                    config.shard_id,
                    "peaks",
                    "input_error",
                    changed_fields=["input"],
                )
            error_payload = _write_error(
                layout,
                store,
                config,
                error,
                config_version=config_version,
                sample_digest=sample_digest,
            )
            errors[config.shard_id] = error_payload
            dataset_entries.append(_dataset_entry(config))
            processed += 1
        if stop_after_shards is not None and processed >= stop_after_shards:
            return RunResult(
                True,
                "stop_after_shards",
                processed,
                len(manifest.shards),
                None,
                None,
                tuple(errors[key] for key in sorted(errors)),
            )

    dataset_digest = version_fingerprint("audiofp.dataset.v1", dataset_entries)
    config_versions = {
        config.shard_id: config.config_version(sha256_file(config.reference_path))
        for config in manifest.shards
        if Path(config.reference_path).exists()
    }
    summary = build_summary(
        shard_order=[config.shard_id for config in manifest.shards],
        config_versions=config_versions,
        audits=audits,
        errors=errors,
        dataset_digest=dataset_digest,
    )
    with FileLock(layout.workspace / ".locks" / "summary.lock"):
        summary_bytes = atomic_write_json(layout.summary_path(), summary)
        summary_digest = sha256_bytes(summary_bytes)
        atomic_write_bytes(layout.summary_digest_path(), (summary_digest + "\n").encode("ascii"))
    return RunResult(
        False,
        None,
        processed,
        len(manifest.shards),
        str(layout.summary_path()),
        summary_digest,
        tuple(errors[key] for key in sorted(errors)),
    )


def resume_pipeline(
    workspace: str | Path,
    manifest_path: str | None = None,
    *,
    stop_after_stage: str | None = None,
    stop_after_shards: int | None = None,
) -> RunResult:
    return run_pipeline(
        workspace,
        manifest_path,
        stop_after_stage=stop_after_stage,
        stop_after_shards=stop_after_shards,
        clear_stop_request=True,
    )

