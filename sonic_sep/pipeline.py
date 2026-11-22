"""Five-stage build, incremental bandwidth update, stop/resume, and validation."""

from __future__ import annotations

import copy
import os
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import numpy as np
import soundfile as sf

from .checkpoints import CheckpointStore, checkpoint_status
from .config import SeparationConfig
from .errors import (
    AppError,
    conflict,
    incremental_mismatch,
    invalid_config,
    numeric_error,
    not_found,
)
from .features import (
    BandFeatures,
    extract_features,
    feature_digest,
    features_input_fingerprint,
    frequency_bin_to_band,
    band_edges,
)
from .manifest import AudioManifest, SliceTruth, SourceTruth, load_slice_audio
from .masks import (
    MaskState,
    changed_bands_for_sources,
    initialize_masks,
    mask_digest,
    masks_input_fingerprint,
    refinement_input_fingerprint,
    refine_round,
    source_priors,
)
from .reconstruct import (
    ReconstructionBlock,
    ProcessBlock,
    assemble_waveforms,
    compute_ola_norm,
    full_result_digest,
    make_process_blocks,
    make_reconstruction_blocks,
    reconstruct_block,
    reconstruct_block_digest,
    reconstruction_input_fingerprint,
    round_changes_digest,
)
from .stft import (
    StftResult,
    block_stft_digest,
    compute_stft,
    frame_count,
    stft_input_fingerprint,
)
from .util import (
    atomic_write_json,
    atomic_write_npz,
    digest_array,
    digest_json,
    json_safe,
    load_npz,
    read_json,
)
from .workspace import Workspace


STAGE_ORDER = ("stft", "features", "mask_init", "mask_refine", "reconstruct")


@dataclass
class BuildStopped(Exception):
    reason: str
    units_completed: int

    def __post_init__(self) -> None:
        super().__init__(self.reason)


class UnitGate:
    def __init__(self, workspace: Workspace, stop_after_units: int | None):
        self.workspace = workspace
        self.stop_after_units = stop_after_units
        self.units = 0

    def before(self) -> None:
        if self.workspace.stop_requested():
            raise BuildStopped("stop_requested", self.units)

    def after(self) -> None:
        self.units += 1
        if self.stop_after_units is not None and self.units >= self.stop_after_units:
            self.workspace.request_stop("stop_after_units")
            raise BuildStopped("stop_after_units", self.units)


def _safe_completed(meta: Mapping[str, Any]) -> set[int]:
    values = meta.get("completed_blocks", [])
    if not isinstance(values, list):
        return set()
    return {int(value) for value in values if isinstance(value, int)}


def _meta_matches(
    meta: Mapping[str, Any],
    arrays: Mapping[str, np.ndarray],
    *,
    input_fingerprint: str,
    expected_shapes: Mapping[str, tuple[int, ...]],
) -> bool:
    if meta.get("input_fingerprint") != input_fingerprint:
        return False
    for name, shape in expected_shapes.items():
        if name not in arrays or tuple(arrays[name].shape) != tuple(shape):
            return False
    return True


def _load_optional_npz(path: Path) -> tuple[dict[str, np.ndarray], dict[str, Any]] | None:
    if not path.is_file():
        return None
    try:
        return load_npz(path)
    except (OSError, ValueError, KeyError, EOFError):
        return None


def _save_wav_atomic(path: Path, pcm: np.ndarray, sample_rate: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    os.close(fd)
    try:
        sf.write(temp_name, pcm, sample_rate, format="WAV", subtype="PCM_16")
        with open(temp_name, "rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass
    except BaseException:
        try:
            os.unlink(temp_name)
        except FileNotFoundError:
            pass
        raise


def _source_payload(sources: Sequence[SourceTruth]) -> list[dict[str, Any]]:
    return [
        {
            "source_id": int(source.source_id),
            "f0_min_hz": float(source.f0_min_hz),
            "f0_max_hz": float(source.f0_max_hz),
            "visible": bool(source.visible),
        }
        for source in sources
    ]


def _sources_from_payload(payload: Any) -> tuple[SourceTruth, ...]:
    if not isinstance(payload, list):
        return ()
    result: list[SourceTruth] = []
    for item in payload:
        if not isinstance(item, Mapping):
            return ()
        try:
            result.append(
                SourceTruth(
                    int(item["source_id"]),
                    float(item["f0_min_hz"]),
                    float(item["f0_max_hz"]),
                    bool(item.get("visible", True)),
                )
            )
        except (KeyError, TypeError, ValueError):
            return ()
    return tuple(result)


class SeparationPipeline:
    def __init__(self, workspace: Workspace):
        self.workspace = workspace
        self.checkpoints = CheckpointStore(workspace.state_root)

    # ------------------------------------------------------------------
    # Public operations
    # ------------------------------------------------------------------
    def build_all(
        self,
        *,
        slice_ids: Iterable[str] | None = None,
        stop_after_units: int | None = None,
    ) -> dict[str, Any]:
        manifest = self.workspace.effective_manifest()
        selected = list(slice_ids) if slice_ids else manifest.slice_ids()
        if selected == ["all"]:
            selected = manifest.slice_ids()
        for slice_id in selected:
            manifest.get(slice_id)
        gate = UnitGate(self.workspace, stop_after_units)
        results: list[dict[str, Any]] = []
        for index, slice_id in enumerate(selected):
            try:
                result = self.build_slice(slice_id, gate=gate)
                results.append(result)
            except BuildStopped as stopped:
                results.append(
                    {
                        "ok": True,
                        "status": "stopped",
                        "slice_id": slice_id,
                        "reason": stopped.reason,
                        "units_completed": stopped.units_completed,
                        "resume_required": True,
                        "remaining_slices": selected[index:],
                    }
                )
                break
        stopped_result = next((item for item in results if item.get("status") == "stopped"), None)
        return {
            "ok": True,
            "status": "stopped" if stopped_result else "complete",
            "slice_ids": selected,
            "units_completed": gate.units,
            "results": results,
            "resume_required": bool(stopped_result),
        }

    def resume(self, *, slice_ids: Iterable[str] | None = None) -> dict[str, Any]:
        self.workspace.clear_stop()
        return self.build_all(slice_ids=slice_ids)

    def build_slice(self, slice_id: str, *, gate: UnitGate | None = None) -> dict[str, Any]:
        gate = gate or UnitGate(self.workspace, None)
        manifest = self.workspace.effective_manifest()
        slice_truth = manifest.get(slice_id)
        config = self._config_for_slice(slice_id)
        if slice_truth.source_count > config.max_sources:
            raise invalid_config(
                "slice source count exceeds max_sources",
                slice_id=slice_id,
                param="max_sources",
            )
        audio = load_slice_audio(slice_truth)
        n_frames = frame_count(slice_truth.n_samples, config.hop_length)
        process_blocks = make_process_blocks(n_frames, config.block_frames)
        stage_inputs = self._stage_inputs(slice_truth, config, manifest)
        invalidation = self._record_invalidation(slice_id, slice_truth, config, stage_inputs)
        caches = self._load_or_create_stages(
            slice_id=slice_id,
            slice_truth=slice_truth,
            config=config,
            audio=audio,
            stage_inputs=stage_inputs,
            process_blocks=process_blocks,
            gate=gate,
        )
        gate.before()
        result = self._finalize(
            slice_id=slice_id,
            slice_truth=slice_truth,
            config=config,
            audio=audio,
            n_frames=n_frames,
            process_blocks=process_blocks,
            stage_inputs=stage_inputs,
            caches=caches,
        )
        self._write_version(
            slice_id=slice_id,
            manifest=manifest,
            config=config,
            stage_inputs=stage_inputs,
            products=caches["stage_products"],
        )
        atomic_write_json(
            self.workspace.slice_dir(slice_id) / "last_build.json",
            {
                "status": "complete",
                "result_digest": result["result_digest"],
                "stage_metrics": caches["stage_metrics"],
                "invalidation": invalidation,
            },
        )
        result["ok"] = True
        result["status"] = "complete"
        result["invalidation"] = invalidation
        result["stage_metrics"] = caches["stage_metrics"]
        return result

    def incremental_band_update(
        self,
        slice_id: str,
        *,
        source_id: int,
        band_start: int,
        band_end: int,
        f0_min_hz: float | None = None,
        f0_max_hz: float | None = None,
        block_start: int | None = None,
        block_end: int | None = None,
        verify: bool = True,
    ) -> dict[str, Any]:
        """Update one source's reference prior over a band range and verify equality."""
        config = self._config_for_slice(slice_id)
        manifest = self.workspace.effective_manifest()
        slice_truth = manifest.get(slice_id)
        audio = load_slice_audio(slice_truth)
        n_frames = frame_count(slice_truth.n_samples, config.hop_length)
        process_blocks = make_process_blocks(n_frames, config.block_frames)
        if isinstance(source_id, bool) or not isinstance(source_id, int) or source_id < 0:
            raise numeric_error(
                "source_id must be a non-negative integer",
                slice_id=slice_id,
                band_index=band_start,
                frame_index=0,
            )
        if source_id >= slice_truth.source_count:
            raise not_found("source does not exist in this slice", slice_id=slice_id, source_id=source_id)
        if not (0 <= band_start <= band_end < config.band_count):
            raise numeric_error(
                "incremental band range is outside band_count",
                slice_id=slice_id,
                band_index=max(0, min(band_start, config.band_count - 1)),
                frame_index=0,
            )
        if block_start is not None or block_end is not None:
            lo = 0 if block_start is None else int(block_start)
            hi = len(process_blocks) - 1 if block_end is None else int(block_end)
            if not (0 <= lo <= hi < len(process_blocks)):
                raise invalid_config(
                    "incremental block range is outside the available blocks",
                    slice_id=slice_id,
                    param="blocks",
                    block_count=len(process_blocks),
                )
        caches = self._load_complete_caches(slice_id, slice_truth, config, audio, process_blocks)
        old_sources = list(slice_truth.sources)
        new_sources = copy.deepcopy(old_sources)
        source_updated = False
        if f0_min_hz is not None or f0_max_hz is not None:
            target = new_sources[source_id]
            new_min = float(target.f0_min_hz if f0_min_hz is None else f0_min_hz)
            new_max = float(target.f0_max_hz if f0_max_hz is None else f0_max_hz)
            if not np.isfinite(new_min) or not np.isfinite(new_max) or new_min <= 0 or new_max <= 0:
                raise numeric_error(
                    "updated f0 interval must be finite and positive",
                    slice_id=slice_id,
                    band_index=band_start,
                    frame_index=0,
                )
            if new_max < new_min:
                raise numeric_error(
                    "updated f0 upper bound must not be below lower bound",
                    slice_id=slice_id,
                    band_index=band_start,
                    frame_index=0,
                )
            new_sources[source_id] = SourceTruth(source_id, new_min, new_max, target.visible)
            source_updated = (new_min, new_max) != (target.f0_min_hz, target.f0_max_hz)
        requested_bands = list(range(band_start, band_end + 1))
        prior_changed = changed_bands_for_sources(old_sources, new_sources, config) if source_updated else []
        affected_bands = sorted(set(requested_bands) | set(prior_changed))
        if source_updated and not affected_bands:
            affected_bands = requested_bands
        stage_inputs = self._stage_inputs_for_sources(slice_truth, config, tuple(new_sources))
        masks_input_fp = stage_inputs["mask_init"]
        refine_input_fp = stage_inputs["mask_refine"]
        recon_input_fp = stage_inputs["reconstruct"]
        stage_metrics = {
            "stft": {"status": "reused", "bands_recomputed": [], "blocks_recomputed": []},
            "features": {"status": "reused", "bands_recomputed": [], "blocks_recomputed": []},
            "mask_init": {
                "status": "recomputed",
                "bands_recomputed": affected_bands,
                "blocks_recomputed": [block.index for block in process_blocks],
            },
            "mask_refine": {
                "status": "recomputed",
                "bands_recomputed": affected_bands,
                "blocks_recomputed": [block.index for block in process_blocks],
            },
            "reconstruct": {
                "status": "recomputed",
                "bands_recomputed": affected_bands,
                "blocks_recomputed": [block.index for block in process_blocks],
            },
        }
        masks_init = caches["masks_init"].copy()
        new_mask_state = initialize_masks(
            caches["features"],
            tuple(new_sources),
            config,
            slice_id=slice_id,
            global_frame_indices=np.arange(n_frames, dtype=np.int64),
            band_indices=affected_bands,
            existing=masks_init,
        )
        masks_init = new_mask_state.masks
        masks = caches["masks"].copy()
        masks[:, :, affected_bands] = masks_init[:, :, affected_bands]
        round_changes = caches["round_changes"].copy()
        rounds_done = caches["rounds_done"].copy()
        for block in process_blocks:
            frames = np.arange(block.frame_start, block.frame_end, dtype=np.int64)
            selected = masks[:, block.frame_start : block.frame_end, :]
            for round_index in range(config.mask_iterations):
                selected, changes = refine_round(
                    selected,
                    caches["features"].features[block.frame_start : block.frame_end],
                    tuple(new_sources),
                    config,
                    slice_id=slice_id,
                    global_frame_indices=frames,
                    band_indices=affected_bands,
                )
                masks[:, block.frame_start : block.frame_end, affected_bands] = selected[:, :, affected_bands]
                round_changes[block.index, round_index, affected_bands] = changes
            rounds_done[block.index] = config.mask_iterations
        # Reconstruct every block because a frequency-domain mask applies to all time frames.
        recon_blocks = make_reconstruction_blocks(process_blocks, n_frames, slice_truth.n_samples, config)
        norm = compute_ola_norm(recon_blocks, slice_truth.n_samples, config)
        contributions: list[np.ndarray] = []
        plan_digest = self._reconstruction_plan_digest(recon_blocks, slice_truth, config)
        for block in recon_blocks:
            contribution = reconstruct_block(
                block,
                caches["stft"].spectra,
                masks,
                slice_id=slice_id,
                config=config,
            )
            contributions.append(contribution)
        float_waveforms, waveforms, clipping, recon_digest = assemble_waveforms(
            recon_blocks,
            contributions,
            norm,
            slice_truth.n_samples,
            slice_id=slice_id,
        )
        global_round_changes = np.sum(round_changes, axis=0)
        result_digest = full_result_digest(waveforms, masks, global_round_changes, clipping)
        candidate = {
            "stft": caches["stft"],
            "features": caches["features"],
            "masks_init": masks_init,
            "masks": masks,
            "round_changes": round_changes,
            "rounds_done": rounds_done,
            "recon_blocks": recon_blocks,
            "contributions": contributions,
            "norm": norm,
            "float_waveforms": float_waveforms,
            "waveforms": waveforms,
            "clipping": clipping,
            "recon_digest": recon_digest,
            "result_digest": result_digest,
            "global_round_changes": global_round_changes,
            "recon_input_fingerprint": recon_input_fp,
            "plan_digest": plan_digest,
        }
        if verify:
            reference = self._compute_reference(slice_truth, config, audio, tuple(new_sources))
            self._verify_incremental(slice_id, config, process_blocks, candidate, reference)
        products = {
            "stft": caches["stft"].digest,
            "features": caches["features"].digest,
            "mask_init": mask_digest(masks_init),
            "mask_refine": digest_json(
                {
                    "masks": mask_digest(masks),
                    "round_changes": round_changes_digest(global_round_changes),
                }
            ),
            "reconstruct": result_digest,
        }
        invalidation = self._append_invalidation(
            slice_id=slice_id,
            reason="incremental_source_f0" if source_updated else "incremental_band_mask",
            earliest_stage="mask_init",
            affected_bands=affected_bands,
            from_fingerprint=self._current_version_fingerprint(slice_id),
            to_fingerprint=digest_json(stage_inputs),
        )
        self._persist_incremental(
            slice_id=slice_id,
            slice_truth=slice_truth,
            config=config,
            caches=caches,
            candidate=candidate,
            affected_bands=affected_bands,
            masks_input_fp=masks_input_fp,
            refine_input_fp=refine_input_fp,
            recon_input_fp=recon_input_fp,
            sources=tuple(new_sources),
            products=products,
            invalidation=invalidation,
            source_updated=source_updated,
            source_id=source_id,
            f0_min_hz=new_sources[source_id].f0_min_hz if source_updated else None,
            f0_max_hz=new_sources[source_id].f0_max_hz if source_updated else None,
        )
        atomic_write_json(
            self.workspace.slice_dir(slice_id) / "last_build.json",
            {
                "status": "complete",
                "mode": "incremental",
                "result_digest": result_digest,
                "stage_metrics": stage_metrics,
                "invalidation": invalidation,
            },
        )
        return {
            "ok": True,
            "status": "complete",
            "slice_id": slice_id,
            "source_id": source_id,
            "affected_bands": affected_bands,
            "rebuilt_blocks": [block.index for block in recon_blocks],
            "incremental_verified": bool(verify),
            "full_equivalent": True,
            "stage_metrics": stage_metrics,
            "mask_digest": mask_digest(masks),
            "round_changes_digest": round_changes_digest(global_round_changes),
            "result_digest": result_digest,
            "source_sample_counts": [int(waveforms[index].size) for index in range(len(new_sources))],
            "clipping": clipping,
            "invalidation": invalidation,
        }

    def verify_slice(self, slice_id: str) -> dict[str, Any]:
        config = self.workspace.config()
        manifest = self.workspace.effective_manifest()
        slice_truth = manifest.get(slice_id)
        audio = load_slice_audio(slice_truth)
        reference = self._compute_reference(slice_truth, config, audio, slice_truth.sources)
        summary_path = self.workspace.slice_dir(slice_id) / "summary.json"
        if not summary_path.is_file():
            raise conflict("slice has no completed summary to verify", slice_id=slice_id)
        summary = read_json(summary_path)
        checks = {
            "result_digest": summary.get("result_digest") == reference["result_digest"],
            "mask_digest": summary.get("mask_digest") == mask_digest(reference["masks"]),
            "round_changes_digest": summary.get("round_changes_digest")
            == round_changes_digest(reference["global_round_changes"]),
        }
        if not all(checks.values()):
            raise incremental_mismatch(
                "persisted slice does not match a fresh full computation",
                slice_id=slice_id,
                band_index=0,
                block_index=0,
                stage="verify",
            )
        return {
            "ok": True,
            "slice_id": slice_id,
            "fresh_full_match": True,
            "checks": checks,
            "result_digest": reference["result_digest"],
            "reference": reference["summary"],
        }

    def status(self, *, slice_id: str | None = None) -> dict[str, Any]:
        workspace_summary = self.workspace.summary()
        if not workspace_summary.get("imported"):
            return {"ok": True, "workspace": workspace_summary, "slices": []}
        manifest = self.workspace.effective_manifest()
        config = self.workspace.config()
        ids = [slice_id] if slice_id else manifest.slice_ids()
        rows = []
        for sid in ids:
            truth = manifest.get(sid)
            summary_path = self.workspace.slice_dir(sid) / "summary.json"
            version_path = self.workspace.slice_dir(sid) / "version.json"
            summary = read_json(summary_path) if summary_path.is_file() else None
            version = read_json(version_path) if version_path.is_file() else None
            current_stage_inputs = self._stage_inputs(truth, config, manifest)
            version_current = bool(
                version and version.get("stage_inputs") == current_stage_inputs
            )
            last_build_path = self.workspace.slice_dir(sid) / "last_build.json"
            last_build = read_json(last_build_path) if last_build_path.is_file() else None
            rows.append(
                {
                    "slice_id": sid,
                    "n_samples": truth.n_samples,
                    "duration_seconds": truth.duration_seconds,
                    "source_count": truth.source_count,
                    "sources": _source_payload(truth.sources),
                    "complete": summary is not None and version_current,
                    "has_summary": summary is not None,
                    "version_current": version_current,
                    "current_stage_inputs": current_stage_inputs,
                    "result_digest": summary.get("result_digest") if summary else None,
                    "mask_digest": summary.get("mask_digest") if summary else None,
                    "round_changes_digest": summary.get("round_changes_digest") if summary else None,
                    "clipping": summary.get("clipping") if summary else None,
                    "stage_products": summary.get("stage_products") if summary else None,
                    "version": version,
                    "last_build": last_build,
                    "outputs": summary.get("outputs") if summary else None,
                }
            )
        return {
            "ok": True,
            "workspace": workspace_summary,
            "config": config.to_dict(),
            "slices": rows,
            "checkpoints": checkpoint_status(self.checkpoints),
            "stop_requested": self.workspace.stop_requested(),
        }

    def version(self, *, slice_id: str | None = None) -> dict[str, Any]:
        manifest = self.workspace.effective_manifest()
        config = self.workspace.config()
        ids = [slice_id] if slice_id else manifest.slice_ids()
        rows = []
        for sid in ids:
            truth = manifest.get(sid)
            current = self._stage_inputs(truth, config, manifest)
            path = self.workspace.slice_dir(sid) / "version.json"
            persisted = read_json(path) if path.is_file() else None
            rows.append(
                {
                    "slice_id": sid,
                    "config_digest": digest_json(config.to_dict()),
                    "manifest_digest": manifest.digest(),
                    "current_stage_inputs": current,
                    "current_fingerprint": digest_json(current),
                    "persisted": persisted,
                    "matches_persisted": bool(
                        persisted and persisted.get("stage_inputs") == current
                    ),
                }
            )
        return {"ok": True, "slices": rows}

    def invalidations(self, *, slice_id: str | None = None) -> dict[str, Any]:
        manifest = self.workspace.effective_manifest()
        ids = [slice_id] if slice_id else manifest.slice_ids()
        rows = []
        for sid in ids:
            path = self.workspace.slice_dir(sid) / "invalidations.json"
            records = read_json(path) if path.is_file() else []
            rows.append({"slice_id": sid, "records": records})
        return {"ok": True, "slices": rows}

    def mask_changes(self, *, slice_id: str | None = None) -> dict[str, Any]:
        manifest = self.workspace.effective_manifest()
        ids = [slice_id] if slice_id else manifest.slice_ids()
        rows = []
        for sid in ids:
            path = self.workspace.slice_dir(sid) / "summary.json"
            if not path.is_file():
                rows.append({"slice_id": sid, "complete": False, "round_changes": None})
                continue
            summary = read_json(path)
            rows.append(
                {
                    "slice_id": sid,
                    "complete": True,
                    "rounds": summary.get("round_changes"),
                    "round_changes_digest": summary.get("round_changes_digest"),
                }
            )
        return {"ok": True, "slices": rows}

    # ------------------------------------------------------------------
    # Stage cache construction
    # ------------------------------------------------------------------
    def _stage_inputs(
        self,
        slice_truth: SliceTruth,
        config: SeparationConfig,
        manifest: AudioManifest,
    ) -> dict[str, str]:
        return self._stage_inputs_for_sources(slice_truth, config, slice_truth.sources)

    def _stage_inputs_for_sources(
        self,
        slice_truth: SliceTruth,
        config: SeparationConfig,
        sources: Sequence[SourceTruth],
    ) -> dict[str, str]:
        stft = stft_input_fingerprint(slice_truth, config)
        features = features_input_fingerprint(stft, config)
        mask_init = masks_input_fingerprint(features, sources, config)
        refine = refinement_input_fingerprint(mask_init, config)
        recon = reconstruction_input_fingerprint(refine, config)
        return {
            "stft": stft,
            "features": features,
            "mask_init": mask_init,
            "mask_refine": refine,
            "reconstruct": recon,
        }

    def _load_or_create_stages(
        self,
        *,
        slice_id: str,
        slice_truth: SliceTruth,
        config: SeparationConfig,
        audio: np.ndarray,
        stage_inputs: Mapping[str, str],
        process_blocks: Sequence[ProcessBlock],
        gate: UnitGate,
    ) -> dict[str, Any]:
        cache_dir = self.workspace.cache_dir(slice_id)
        n_frames = frame_count(slice_truth.n_samples, config.hop_length)
        metrics: dict[str, Any] = {}

        stft = self._ensure_stft(
            slice_id=slice_id,
            slice_truth=slice_truth,
            config=config,
            audio=audio,
            input_fp=stage_inputs["stft"],
            process_blocks=process_blocks,
            cache_dir=cache_dir,
            n_frames=n_frames,
            gate=gate,
            metrics=metrics,
        )
        features = self._ensure_features(
            slice_id=slice_id,
            config=config,
            stft=stft,
            input_fp=stage_inputs["features"],
            process_blocks=process_blocks,
            cache_dir=cache_dir,
            gate=gate,
            metrics=metrics,
        )
        mask_init = self._ensure_masks_init(
            slice_id=slice_id,
            slice_truth=slice_truth,
            config=config,
            features=features,
            input_fp=stage_inputs["mask_init"],
            process_blocks=process_blocks,
            cache_dir=cache_dir,
            gate=gate,
            metrics=metrics,
        )
        masks, round_changes, rounds_done = self._ensure_refinement(
            slice_id=slice_id,
            slice_truth=slice_truth,
            config=config,
            features=features,
            mask_init=mask_init,
            input_fp=stage_inputs["mask_refine"],
            process_blocks=process_blocks,
            cache_dir=cache_dir,
            gate=gate,
            metrics=metrics,
        )
        recon_blocks = make_reconstruction_blocks(process_blocks, n_frames, slice_truth.n_samples, config)
        plan_digest = self._reconstruction_plan_digest(recon_blocks, slice_truth, config)
        norm = compute_ola_norm(recon_blocks, slice_truth.n_samples, config)
        self._ensure_reconstruction_blocks(
            slice_id=slice_id,
            slice_truth=slice_truth,
            config=config,
            stft=stft,
            masks=masks,
            input_fp=stage_inputs["reconstruct"],
            plan_digest=plan_digest,
            recon_blocks=recon_blocks,
            gate=gate,
            metrics=metrics,
        )
        return {
            "stft": stft,
            "features": features,
            "masks_init": mask_init,
            "masks": masks,
            "round_changes": round_changes,
            "rounds_done": rounds_done,
            "recon_blocks": recon_blocks,
            "norm": norm,
            "plan_digest": plan_digest,
            "stage_metrics": metrics,
            "stage_products": {},
        }

    def _ensure_stft(
        self,
        *,
        slice_id: str,
        slice_truth: SliceTruth,
        config: SeparationConfig,
        audio: np.ndarray,
        input_fp: str,
        process_blocks: Sequence[ProcessBlock],
        cache_dir: Path,
        n_frames: int,
        gate: UnitGate,
        metrics: dict[str, Any],
    ) -> StftResult:
        path = cache_dir / "stft.npz"
        n_bins = config.frame_length // 2 + 1
        loaded = _load_optional_npz(path)
        expected_shapes = {
            "spectra": (n_frames, n_bins),
            "frame_indices": (n_frames,),
            "frame_starts": (n_frames,),
            "valid_lengths": (n_frames,),
            "padded": (n_frames,),
        }
        if loaded and _meta_matches(loaded[1], loaded[0], input_fingerprint=input_fp, expected_shapes=expected_shapes):
            arrays, meta = loaded
            if meta.get("block_frames") == config.block_frames:
                completed = _safe_completed(meta)
                block_digests = {str(key): str(value) for key, value in meta.get("block_digests", {}).items()}
            else:
                spectra = np.zeros((n_frames, n_bins), dtype=np.complex128)
                arrays = {"spectra": spectra}
                completed = set()
                block_digests = {}
        else:
            arrays = {}
            completed = set()
            block_digests = {}
        if "spectra" not in arrays:
            arrays = {
                "spectra": np.zeros((n_frames, n_bins), dtype=np.complex128),
                "frame_indices": np.arange(n_frames, dtype=np.int64),
                "frame_starts": np.arange(n_frames, dtype=np.int64) * config.hop_length,
                "valid_lengths": np.minimum(
                    config.frame_length,
                    np.maximum(0, slice_truth.n_samples - np.arange(n_frames, dtype=np.int64) * config.hop_length),
                ).astype(np.int64),
                "padded": (
                    np.arange(n_frames, dtype=np.int64) * config.hop_length + config.frame_length
                    > slice_truth.n_samples
                ),
            }
        else:
            arrays["frame_indices"] = np.arange(n_frames, dtype=np.int64)
            arrays["frame_starts"] = np.arange(n_frames, dtype=np.int64) * config.hop_length
            arrays["valid_lengths"] = np.minimum(
                config.frame_length,
                np.maximum(0, slice_truth.n_samples - arrays["frame_starts"]),
            ).astype(np.int64)
            arrays["padded"] = arrays["frame_starts"] + config.frame_length > slice_truth.n_samples
        reused_before = len(completed)
        recomputed: list[int] = []
        for block in process_blocks:
            gate.before()
            if block.index in completed:
                continue
            result = compute_stft(
                audio,
                config,
                slice_id=slice_id,
                frame_indices=np.arange(block.frame_start, block.frame_end, dtype=np.int64),
            )
            arrays["spectra"][block.frame_start : block.frame_end] = result.spectra
            arrays["frame_indices"][block.frame_start : block.frame_end] = result.frame_indices
            block_digest = block_stft_digest(
                result.spectra,
                result.frame_starts,
                result.padded,
            )
            block_digests[str(block.index)] = block_digest
            completed.add(block.index)
            recomputed.append(block.index)
            meta = self._cache_meta(
                stage="stft",
                input_fp=input_fp,
                config=config,
                completed=completed,
                block_digests=block_digests,
                extra={"n_frames": n_frames, "n_samples": slice_truth.n_samples, "block_frames": config.block_frames},
            )
            atomic_write_npz(path, arrays, meta)
            self.checkpoints.publish_unit(
                slice_id=slice_id,
                stage="stft",
                block_index=block.index,
                frame_start=block.frame_start,
                frame_end=block.frame_end,
                bands=(0, config.band_count - 1),
                input_fingerprint=input_fp,
                config_version=config.fingerprint(),
                product_digest=block_digest,
            )
            gate.after()
        full_digest = self._stft_full_digest(arrays)
        status = "reused" if reused_before == len(process_blocks) else ("partial" if reused_before else "recomputed")
        metrics["stft"] = {
            "status": status,
            "blocks_recomputed": recomputed,
            "bands_recomputed": [],
            "product_digest": full_digest,
        }
        return StftResult(
            spectra=arrays["spectra"],
            frame_indices=arrays["frame_indices"],
            frame_starts=arrays["frame_starts"],
            valid_lengths=arrays["valid_lengths"],
            padded=arrays["padded"],
            window=np.hanning(config.frame_length).astype(np.float64),
            n_samples=slice_truth.n_samples,
            digest=full_digest,
        )

    def _ensure_features(
        self,
        *,
        slice_id: str,
        config: SeparationConfig,
        stft: StftResult,
        input_fp: str,
        process_blocks: Sequence[ProcessBlock],
        cache_dir: Path,
        gate: UnitGate,
        metrics: dict[str, Any],
    ) -> BandFeatures:
        path = cache_dir / "features.npz"
        n_frames = stft.spectra.shape[0]
        loaded = _load_optional_npz(path)
        expected_shapes = {
            "features": (n_frames, config.band_count, 2),
            "edge_hz": (config.band_count + 1,),
            "bin_band": (config.frame_length // 2 + 1,),
        }
        if loaded and _meta_matches(loaded[1], loaded[0], input_fingerprint=input_fp, expected_shapes=expected_shapes):
            arrays, meta = loaded
            if meta.get("block_frames") == config.block_frames:
                completed = _safe_completed(meta)
                block_digests = {str(key): str(value) for key, value in meta.get("block_digests", {}).items()}
            else:
                completed = set()
                block_digests = {}
                arrays = {}
        else:
            arrays = {}
            completed = set()
            block_digests = {}
        if "features" not in arrays:
            arrays = {
                "features": np.zeros((n_frames, config.band_count, 2), dtype=np.float64),
                "edge_hz": band_edges(config),
                "bin_band": frequency_bin_to_band(config),
            }
        reused_before = len(completed)
        recomputed: list[int] = []
        for block in process_blocks:
            gate.before()
            if block.index in completed:
                continue
            block_features = extract_features(
                stft.spectra[block.frame_start : block.frame_end],
                config,
                slice_id=slice_id,
                frame_indices=np.arange(block.frame_start, block.frame_end, dtype=np.int64),
            )
            arrays["features"][block.frame_start : block.frame_end] = block_features.features
            block_digest = feature_digest(
                block_features.features,
                block_features.band_edges_hz,
                block_features.bin_band,
            )
            block_digests[str(block.index)] = block_digest
            completed.add(block.index)
            recomputed.append(block.index)
            meta = self._cache_meta(
                stage="features",
                input_fp=input_fp,
                config=config,
                completed=completed,
                block_digests=block_digests,
                extra={"n_frames": n_frames, "block_frames": config.block_frames},
            )
            atomic_write_npz(path, arrays, meta)
            self.checkpoints.publish_unit(
                slice_id=slice_id,
                stage="features",
                block_index=block.index,
                frame_start=block.frame_start,
                frame_end=block.frame_end,
                bands=(0, config.band_count - 1),
                input_fingerprint=input_fp,
                config_version=config.fingerprint(),
                product_digest=block_digest,
            )
            gate.after()
        full_digest = feature_digest(arrays["features"], arrays["edge_hz"], arrays["bin_band"])
        status = "reused" if reused_before == len(process_blocks) else ("partial" if reused_before else "recomputed")
        metrics["features"] = {
            "status": status,
            "blocks_recomputed": recomputed,
            "bands_recomputed": [],
            "product_digest": full_digest,
        }
        return BandFeatures(
            features=arrays["features"],
            band_edges_hz=arrays["edge_hz"],
            bin_band=arrays["bin_band"],
            digest=full_digest,
        )

    def _ensure_masks_init(
        self,
        *,
        slice_id: str,
        slice_truth: SliceTruth,
        config: SeparationConfig,
        features: BandFeatures,
        input_fp: str,
        process_blocks: Sequence[ProcessBlock],
        cache_dir: Path,
        gate: UnitGate,
        metrics: dict[str, Any],
    ) -> np.ndarray:
        path = cache_dir / "masks_init.npz"
        n_frames = features.features.shape[0]
        n_sources = slice_truth.source_count
        loaded = _load_optional_npz(path)
        expected_shapes = {"masks": (n_sources, n_frames, config.band_count)}
        if loaded and _meta_matches(loaded[1], loaded[0], input_fingerprint=input_fp, expected_shapes=expected_shapes):
            arrays, meta = loaded
            if meta.get("block_frames") == config.block_frames:
                completed = _safe_completed(meta)
                block_digests = {str(key): str(value) for key, value in meta.get("block_digests", {}).items()}
            else:
                completed = set()
                block_digests = {}
                arrays = {}
        else:
            arrays = {}
            completed = set()
            block_digests = {}
        if "masks" not in arrays:
            arrays = {"masks": np.zeros((n_sources, n_frames, config.band_count), dtype=np.float64)}
        reused_before = len(completed)
        recomputed: list[int] = []
        for block in process_blocks:
            gate.before()
            if block.index in completed:
                continue
            block_state = initialize_masks(
                BandFeatures(
                    features=features.features[block.frame_start : block.frame_end],
                    band_edges_hz=features.band_edges_hz,
                    bin_band=features.bin_band,
                    digest=features.digest,
                ),
                slice_truth.sources,
                config,
                slice_id=slice_id,
                global_frame_indices=np.arange(block.frame_start, block.frame_end, dtype=np.int64),
            )
            arrays["masks"][:, block.frame_start : block.frame_end, :] = block_state.masks
            block_digest = mask_digest(block_state.masks)
            block_digests[str(block.index)] = block_digest
            completed.add(block.index)
            recomputed.append(block.index)
            meta = self._cache_meta(
                stage="mask_init",
                input_fp=input_fp,
                config=config,
                completed=completed,
                block_digests=block_digests,
                extra={"n_frames": n_frames, "block_frames": config.block_frames},
            )
            atomic_write_npz(path, arrays, meta)
            self.checkpoints.publish_unit(
                slice_id=slice_id,
                stage="mask_init",
                block_index=block.index,
                frame_start=block.frame_start,
                frame_end=block.frame_end,
                bands=(0, config.band_count - 1),
                input_fingerprint=input_fp,
                config_version=config.fingerprint(),
                product_digest=block_digest,
            )
            gate.after()
        full_digest = mask_digest(arrays["masks"])
        status = "reused" if reused_before == len(process_blocks) else ("partial" if reused_before else "recomputed")
        metrics["mask_init"] = {
            "status": status,
            "blocks_recomputed": recomputed,
            "bands_recomputed": [],
            "product_digest": full_digest,
        }
        return arrays["masks"]

    def _ensure_refinement(
        self,
        *,
        slice_id: str,
        slice_truth: SliceTruth,
        config: SeparationConfig,
        features: BandFeatures,
        mask_init: np.ndarray,
        input_fp: str,
        process_blocks: Sequence[ProcessBlock],
        cache_dir: Path,
        gate: UnitGate,
        metrics: dict[str, Any],
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        path = cache_dir / "masks_refine.npz"
        n_frames = features.features.shape[0]
        n_sources = slice_truth.source_count
        n_blocks = len(process_blocks)
        loaded = _load_optional_npz(path)
        expected_shapes = {
            "masks": (n_sources, n_frames, config.band_count),
            "round_changes": (n_blocks, config.mask_iterations, config.band_count),
            "rounds_done": (n_blocks,),
        }
        if loaded and _meta_matches(loaded[1], loaded[0], input_fingerprint=input_fp, expected_shapes=expected_shapes):
            arrays, meta = loaded
            if meta.get("block_frames") == config.block_frames:
                completed = _safe_completed(meta)
                block_digests = {str(key): str(value) for key, value in meta.get("block_digests", {}).items()}
            else:
                completed = set()
                block_digests = {}
                arrays = {}
        else:
            arrays = {}
            completed = set()
            block_digests = {}
        if "masks" not in arrays:
            arrays = {
                "masks": np.array(mask_init, dtype=np.float64, copy=True),
                "round_changes": np.zeros(
                    (n_blocks, config.mask_iterations, config.band_count), dtype=np.float64
                ),
                "rounds_done": np.zeros(n_blocks, dtype=np.int64),
            }
        reused_before = len(completed)
        recomputed: list[int] = []
        for block in process_blocks:
            gate.before()
            frames = np.arange(block.frame_start, block.frame_end, dtype=np.int64)
            while int(arrays["rounds_done"][block.index]) < config.mask_iterations:
                round_index = int(arrays["rounds_done"][block.index])
                selected = arrays["masks"][:, block.frame_start : block.frame_end, :]
                refined, changes = refine_round(
                    selected,
                    features.features[block.frame_start : block.frame_end],
                    slice_truth.sources,
                    config,
                    slice_id=slice_id,
                    global_frame_indices=frames,
                )
                arrays["masks"][:, block.frame_start : block.frame_end, :] = refined
                arrays["round_changes"][block.index, round_index, :] = changes
                arrays["rounds_done"][block.index] = round_index + 1
                if block.index not in recomputed:
                    recomputed.append(block.index)
                block_digest = digest_json(
                    {
                        "masks": digest_array(refined),
                        "round": round_index,
                        "changes": digest_array(changes),
                    }
                )
                block_digests[str(block.index)] = block_digest
                if round_index + 1 >= config.mask_iterations:
                    completed.add(block.index)
                meta = self._cache_meta(
                    stage="mask_refine",
                    input_fp=input_fp,
                    config=config,
                    completed=completed,
                    block_digests=block_digests,
                    extra={
                        "n_frames": n_frames,
                        "block_frames": config.block_frames,
                        "rounds_done": arrays["rounds_done"].tolist(),
                    },
                )
                atomic_write_npz(path, arrays, meta)
                self.checkpoints.publish_unit(
                    slice_id=slice_id,
                    stage="mask_refine",
                    block_index=block.index,
                    frame_start=block.frame_start,
                    frame_end=block.frame_end,
                    bands=(0, config.band_count - 1),
                    input_fingerprint=input_fp,
                    config_version=config.fingerprint(),
                    product_digest=block_digest,
                    round_index=round_index,
                    stage_complete=round_index + 1 >= config.mask_iterations,
                )
                gate.after()
            if config.mask_iterations == 0 and block.index not in completed:
                block_digest = digest_json({"masks": digest_array(arrays["masks"][:, block.frame_start : block.frame_end, :])})
                block_digests[str(block.index)] = block_digest
                completed.add(block.index)
                meta = self._cache_meta(
                    stage="mask_refine",
                    input_fp=input_fp,
                    config=config,
                    completed=completed,
                    block_digests=block_digests,
                    extra={
                        "n_frames": n_frames,
                        "block_frames": config.block_frames,
                        "rounds_done": arrays["rounds_done"].tolist(),
                    },
                )
                atomic_write_npz(path, arrays, meta)
                self.checkpoints.publish_unit(
                    slice_id=slice_id,
                    stage="mask_refine",
                    block_index=block.index,
                    frame_start=block.frame_start,
                    frame_end=block.frame_end,
                    bands=(0, config.band_count - 1),
                    input_fingerprint=input_fp,
                    config_version=config.fingerprint(),
                    product_digest=block_digest,
                    round_index=None,
                    stage_complete=True,
                )
                gate.after()
        status = "reused" if reused_before == len(process_blocks) else ("partial" if reused_before else "recomputed")
        metrics["mask_refine"] = {
            "status": status,
            "blocks_recomputed": recomputed,
            "bands_recomputed": [],
            "product_digest": digest_json(
                {
                    "masks": mask_digest(arrays["masks"]),
                    "round_changes": round_changes_digest(np.sum(arrays["round_changes"], axis=0)),
                }
            ),
        }
        return arrays["masks"], arrays["round_changes"], arrays["rounds_done"]

    def _ensure_reconstruction_blocks(
        self,
        *,
        slice_id: str,
        slice_truth: SliceTruth,
        config: SeparationConfig,
        stft: StftResult,
        masks: np.ndarray,
        input_fp: str,
        plan_digest: str,
        recon_blocks: Sequence[ReconstructionBlock],
        gate: UnitGate,
        metrics: dict[str, Any],
    ) -> None:
        directory = self.workspace.recon_dir(slice_id)
        reused = 0
        recomputed: list[int] = []
        for block in recon_blocks:
            gate.before()
            path = directory / f"b{block.index:06d}.npz"
            loaded = _load_optional_npz(path)
            if loaded:
                arrays, meta = loaded
                if (
                    meta.get("input_fingerprint") == input_fp
                    and meta.get("plan_digest") == plan_digest
                    and "contribution" in arrays
                    and tuple(arrays["contribution"].shape) == (slice_truth.source_count, block.local_length)
                    and meta.get("complete") is True
                ):
                    reused += 1
                    continue
            contribution = reconstruct_block(
                block,
                stft.spectra,
                masks,
                slice_id=slice_id,
                config=config,
            )
            recomputed.append(block.index)
            product_digest = reconstruct_block_digest(contribution, block)
            meta = {
                "stage": "reconstruct",
                "slice_id": slice_id,
                "input_fingerprint": input_fp,
                "plan_digest": plan_digest,
                "sample_offset": block.sample_offset,
                "local_length": block.local_length,
                "product_digest": product_digest,
                "complete": True,
            }
            atomic_write_npz(path, {"contribution": contribution}, meta)
            self.checkpoints.publish_unit(
                slice_id=slice_id,
                stage="reconstruct",
                block_index=block.index,
                frame_start=block.frame_start,
                frame_end=block.frame_end,
                bands=(0, config.band_count - 1),
                input_fingerprint=input_fp,
                config_version=config.fingerprint(),
                product_digest=product_digest,
            )
            gate.after()
        status = "reused" if reused == len(recon_blocks) else ("partial" if reused else "recomputed")
        metrics["reconstruct"] = {
            "status": status,
            "blocks_recomputed": recomputed,
            "bands_recomputed": [],
            "product_digest": plan_digest,
        }

    # ------------------------------------------------------------------
    # Finalization and pure reference computation
    # ------------------------------------------------------------------
    def _finalize(
        self,
        *,
        slice_id: str,
        slice_truth: SliceTruth,
        config: SeparationConfig,
        audio: np.ndarray,
        n_frames: int,
        process_blocks: Sequence[ProcessBlock],
        stage_inputs: Mapping[str, str],
        caches: dict[str, Any],
    ) -> dict[str, Any]:
        recon_blocks: Sequence[ReconstructionBlock] = caches["recon_blocks"]
        contributions = []
        for block in recon_blocks:
            path = self.workspace.recon_dir(slice_id) / f"b{block.index:06d}.npz"
            loaded = _load_optional_npz(path)
            if not loaded:
                raise conflict("reconstruction block cache is missing", slice_id=slice_id, block_index=block.index)
            contribution = loaded[0].get("contribution")
            if contribution is None:
                raise conflict("reconstruction block cache is incomplete", slice_id=slice_id, block_index=block.index)
            contributions.append(contribution)
        float_waveforms, waveforms, clipping, recon_digest = assemble_waveforms(
            recon_blocks,
            contributions,
            caches["norm"],
            slice_truth.n_samples,
            slice_id=slice_id,
        )
        global_round_changes = np.sum(caches["round_changes"], axis=0)
        result_digest = full_result_digest(waveforms, caches["masks"], global_round_changes, clipping)
        output_dir = self.workspace.output_dir(slice_id)
        outputs = []
        for source in slice_truth.sources:
            file_name = f"source-{source.source_id:03d}.wav"
            path = output_dir / file_name
            _save_wav_atomic(path, waveforms[source.source_id], config.sample_rate)
            outputs.append(
                {
                    "source_id": source.source_id,
                    "file": file_name,
                    "n_samples": int(waveforms[source.source_id].size),
                    "sha256": digest_array(waveforms[source.source_id]),
                }
            )
        stage_products = {
            "stft": caches["stft"].digest,
            "features": caches["features"].digest,
            "mask_init": mask_digest(caches["masks_init"]),
            "mask_refine": digest_json(
                {
                    "masks": mask_digest(caches["masks"]),
                    "round_changes": round_changes_digest(global_round_changes),
                }
            ),
            "reconstruct": result_digest,
        }
        caches["stage_products"] = stage_products
        summary = {
            "schema_version": 1,
            "slice_id": slice_id,
            "n_samples": slice_truth.n_samples,
            "sample_rate": config.sample_rate,
            "source_count": slice_truth.source_count,
            "sources": _source_payload(slice_truth.sources),
            "frame_count": int(n_frames),
            "padded_frame_count": int(np.count_nonzero(caches["stft"].padded)),
            "block_count": len(process_blocks),
            "reconstruction_block_count": len(recon_blocks),
            "band_count": config.band_count,
            "config": config.to_dict(),
            "stage_inputs": dict(stage_inputs),
            "stage_products": stage_products,
            "mask_digest": mask_digest(caches["masks"]),
            "round_changes_digest": round_changes_digest(global_round_changes),
            "round_changes": global_round_changes.tolist(),
            "waveform_digest": digest_array(waveforms),
            "result_digest": result_digest,
            "clipping": clipping,
            "outputs": outputs,
            "source_sample_counts": [int(waveforms[index].size) for index in range(slice_truth.source_count)],
        }
        atomic_write_json(self.workspace.slice_dir(slice_id) / "summary.json", summary)
        return {
            "slice_id": slice_id,
            "result_digest": result_digest,
            "mask_digest": summary["mask_digest"],
            "round_changes_digest": summary["round_changes_digest"],
            "waveform_digest": summary["waveform_digest"],
            "clipping": clipping,
            "source_sample_counts": summary["source_sample_counts"],
            "outputs": outputs,
            "stage_products": stage_products,
            "summary": summary,
        }

    def _compute_reference(
        self,
        slice_truth: SliceTruth,
        config: SeparationConfig,
        audio: np.ndarray,
        sources: Sequence[SourceTruth],
    ) -> dict[str, Any]:
        n_frames = frame_count(slice_truth.n_samples, config.hop_length)
        stft = compute_stft(audio, config, slice_id=slice_truth.slice_id)
        features = extract_features(stft.spectra, config, slice_id=slice_truth.slice_id)
        mask_init_state = initialize_masks(features, sources, config, slice_id=slice_truth.slice_id)
        process_blocks = make_process_blocks(n_frames, config.block_frames)
        masks = np.array(mask_init_state.masks, copy=True)
        round_changes = np.zeros((len(process_blocks), config.mask_iterations, config.band_count), dtype=np.float64)
        for block in process_blocks:
            frames = np.arange(block.frame_start, block.frame_end, dtype=np.int64)
            selected = masks[:, block.frame_start : block.frame_end, :]
            for round_index in range(config.mask_iterations):
                selected, changes = refine_round(
                    selected,
                    features.features[block.frame_start : block.frame_end],
                    sources,
                    config,
                    slice_id=slice_truth.slice_id,
                    global_frame_indices=frames,
                )
                masks[:, block.frame_start : block.frame_end, :] = selected
                round_changes[block.index, round_index, :] = changes
        recon_blocks = make_reconstruction_blocks(process_blocks, n_frames, slice_truth.n_samples, config)
        norm = compute_ola_norm(recon_blocks, slice_truth.n_samples, config)
        contributions = [
            reconstruct_block(block, stft.spectra, masks, slice_id=slice_truth.slice_id, config=config)
            for block in recon_blocks
        ]
        _, waveforms, clipping, recon_digest = assemble_waveforms(
            recon_blocks,
            contributions,
            norm,
            slice_truth.n_samples,
            slice_id=slice_truth.slice_id,
        )
        global_round_changes = np.sum(round_changes, axis=0)
        result_digest = full_result_digest(waveforms, masks, global_round_changes, clipping)
        return {
            "stft": stft,
            "features": features,
            "masks_init": mask_init_state.masks,
            "masks": masks,
            "round_changes": round_changes,
            "global_round_changes": global_round_changes,
            "masks_digest": mask_digest(masks),
            "round_changes_digest": round_changes_digest(global_round_changes),
            "waveforms": waveforms,
            "waveform_digest": digest_array(waveforms),
            "clipping": clipping,
            "recon_blocks": recon_blocks,
            "contributions": contributions,
            "norm": norm,
            "result_digest": result_digest,
            "summary": {
                "result_digest": result_digest,
                "mask_digest": mask_digest(masks),
                "round_changes_digest": round_changes_digest(global_round_changes),
                "waveform_digest": digest_array(waveforms),
                "clipping": clipping,
            },
        }

    def _verify_incremental(
        self,
        slice_id: str,
        config: SeparationConfig,
        process_blocks: Sequence[ProcessBlock],
        candidate: Mapping[str, Any],
        reference: Mapping[str, Any],
    ) -> None:
        if not np.array_equal(candidate["masks"], reference["masks"]):
            differing = np.argwhere(candidate["masks"] != reference["masks"])
            differing = differing[np.lexsort((differing[:, 2], differing[:, 1], differing[:, 0]))]
            _, frame_index, band_index = [int(value) for value in differing[0]]
            block_index = min(frame_index // config.block_frames, len(process_blocks) - 1)
            raise incremental_mismatch(
                "incremental masks differ from a fresh full computation",
                slice_id=slice_id,
                band_index=band_index,
                block_index=block_index,
                stage="mask_refine",
            )
        if not np.array_equal(candidate["global_round_changes"], reference["global_round_changes"]):
            differing = np.argwhere(candidate["global_round_changes"] != reference["global_round_changes"])
            _, band_index = [int(value) for value in differing[0]]
            raise incremental_mismatch(
                "incremental round changes differ from a fresh full computation",
                slice_id=slice_id,
                band_index=band_index,
                block_index=0,
                stage="mask_refine",
            )
        if candidate["result_digest"] != reference["result_digest"]:
            differing = np.argwhere(candidate["waveforms"] != reference["waveforms"])
            if len(differing):
                _, sample_index = [int(value) for value in differing[0]]
                block_index = min(
                    sample_index // max(1, config.block_frames * config.hop_length),
                    len(process_blocks) - 1,
                )
            else:
                block_index = 0
            requested_bands = np.flatnonzero(
                np.any(candidate["masks"] != reference["masks"], axis=(0, 1))
            )
            band_index = int(requested_bands[0]) if len(requested_bands) else 0
            raise incremental_mismatch(
                "incremental waveform digest differs from a fresh full computation",
                slice_id=slice_id,
                band_index=band_index,
                block_index=block_index,
                stage="reconstruct",
            )

    def _persist_incremental(
        self,
        *,
        slice_id: str,
        slice_truth: SliceTruth,
        config: SeparationConfig,
        caches: Mapping[str, Any],
        candidate: Mapping[str, Any],
        affected_bands: Sequence[int],
        masks_input_fp: str,
        refine_input_fp: str,
        recon_input_fp: str,
        sources: Sequence[SourceTruth],
        products: Mapping[str, str],
        invalidation: Mapping[str, Any] | None,
        source_updated: bool,
        source_id: int,
        f0_min_hz: float | None,
        f0_max_hz: float | None,
    ) -> None:
        cache_dir = self.workspace.cache_dir(slice_id)
        n_frames = caches["stft"].spectra.shape[0]
        process_blocks = make_process_blocks(n_frames, config.block_frames)

        init_path = cache_dir / "masks_init.npz"
        loaded_init = _load_optional_npz(init_path)
        init_arrays = loaded_init[0] if loaded_init else {}
        init_arrays["masks"] = candidate["masks_init"]
        init_meta = self._cache_meta(
            stage="mask_init",
            input_fp=masks_input_fp,
            config=config,
            completed={block.index for block in process_blocks},
            block_digests={str(block.index): mask_digest(candidate["masks_init"][:, block.frame_start : block.frame_end, :]) for block in process_blocks},
            extra={"n_frames": n_frames, "block_frames": config.block_frames},
        )
        atomic_write_npz(init_path, init_arrays, init_meta)
        for block in process_blocks:
            block_digest = mask_digest(candidate["masks_init"][:, block.frame_start : block.frame_end, :])
            self.checkpoints.publish_unit(
                slice_id=slice_id,
                stage="mask_init",
                block_index=block.index,
                frame_start=block.frame_start,
                frame_end=block.frame_end,
                bands=(min(affected_bands), max(affected_bands)),
                input_fingerprint=masks_input_fp,
                config_version=config.fingerprint(),
                product_digest=block_digest,
                reject_duplicate_product=False,
            )

        refine_path = cache_dir / "masks_refine.npz"
        refine_meta = self._cache_meta(
            stage="mask_refine",
            input_fp=refine_input_fp,
            config=config,
            completed={block.index for block in process_blocks},
            block_digests={
                str(block.index): digest_json(
                    {
                        "masks": digest_array(candidate["masks"][:, block.frame_start : block.frame_end, :]),
                        "changes": digest_array(candidate["round_changes"][block.index]),
                    }
                )
                for block in process_blocks
            },
            extra={
                "n_frames": n_frames,
                "block_frames": config.block_frames,
                "rounds_done": candidate["rounds_done"].tolist(),
            },
        )
        atomic_write_npz(
            refine_path,
            {
                "masks": candidate["masks"],
                "round_changes": candidate["round_changes"],
                "rounds_done": candidate["rounds_done"],
            },
            refine_meta,
        )
        for block in process_blocks:
            block_digest = digest_json(
                {
                    "masks": digest_array(candidate["masks"][:, block.frame_start : block.frame_end, :]),
                    "changes": digest_array(candidate["round_changes"][block.index]),
                }
            )
            self.checkpoints.publish_unit(
                slice_id=slice_id,
                stage="mask_refine",
                block_index=block.index,
                frame_start=block.frame_start,
                frame_end=block.frame_end,
                bands=(min(affected_bands), max(affected_bands)),
                input_fingerprint=refine_input_fp,
                config_version=config.fingerprint(),
                product_digest=block_digest,
                round_index=config.mask_iterations - 1 if config.mask_iterations else None,
                stage_complete=True,
                reject_duplicate_product=False,
            )

        recon_dir = self.workspace.recon_dir(slice_id)
        for block, contribution in zip(candidate["recon_blocks"], candidate["contributions"]):
            product_digest = reconstruct_block_digest(contribution, block)
            atomic_write_npz(
                recon_dir / f"b{block.index:06d}.npz",
                {"contribution": contribution},
                {
                    "stage": "reconstruct",
                    "slice_id": slice_id,
                    "input_fingerprint": recon_input_fp,
                    "plan_digest": candidate["plan_digest"],
                    "sample_offset": block.sample_offset,
                    "local_length": block.local_length,
                    "product_digest": product_digest,
                    "complete": True,
                },
            )
            self.checkpoints.publish_unit(
                slice_id=slice_id,
                stage="reconstruct",
                block_index=block.index,
                frame_start=block.frame_start,
                frame_end=block.frame_end,
                bands=(min(affected_bands), max(affected_bands)),
                input_fingerprint=recon_input_fp,
                config_version=config.fingerprint(),
                product_digest=product_digest,
                reject_duplicate_product=False,
            )
        output_dir = self.workspace.output_dir(slice_id)
        outputs = []
        for source in sources:
            file_name = f"source-{source.source_id:03d}.wav"
            _save_wav_atomic(output_dir / file_name, candidate["waveforms"][source.source_id], config.sample_rate)
            outputs.append(
                {
                    "source_id": source.source_id,
                    "file": file_name,
                    "n_samples": int(candidate["waveforms"][source.source_id].size),
                    "sha256": digest_array(candidate["waveforms"][source.source_id]),
                }
            )
        summary_path = self.workspace.slice_dir(slice_id) / "summary.json"
        summary = read_json(summary_path) if summary_path.is_file() else {}
        summary.update(
            {
                "slice_id": slice_id,
                "sources": _source_payload(sources),
                "stage_products": dict(products),
                "mask_digest": mask_digest(candidate["masks"]),
                "round_changes_digest": round_changes_digest(candidate["global_round_changes"]),
                "round_changes": candidate["global_round_changes"].tolist(),
                "waveform_digest": digest_array(candidate["waveforms"]),
                "result_digest": candidate["result_digest"],
                "clipping": candidate["clipping"],
                "outputs": outputs,
                "source_sample_counts": [int(candidate["waveforms"][index].size) for index in range(len(sources))],
            }
        )
        atomic_write_json(summary_path, summary)
        if source_updated and f0_min_hz is not None and f0_max_hz is not None:
            overrides = self.workspace.raw_overrides()
            overrides.setdefault(slice_id, {}).setdefault(source_id, {})
            overrides[slice_id][source_id]["f0_min_hz"] = float(f0_min_hz)
            overrides[slice_id][source_id]["f0_max_hz"] = float(f0_max_hz)
            self.workspace.save_overrides(overrides)
        manifest = self.workspace.effective_manifest()
        stage_inputs = self._stage_inputs_for_sources(slice_truth, config, sources)
        self._write_version(
            slice_id=slice_id,
            manifest=manifest,
            config=config,
            stage_inputs=stage_inputs,
            products=products,
            sources=sources,
        )

    # ------------------------------------------------------------------
    # Versioning and invalidation
    # ------------------------------------------------------------------
    def _stage_inputs_for_manifest(self, manifest: AudioManifest) -> dict[str, dict[str, str]]:
        config = self.workspace.config()
        return {
            item.slice_id: self._stage_inputs(item, config, manifest)
            for item in manifest.slices
        }

    def _record_invalidation(
        self,
        slice_id: str,
        slice_truth: SliceTruth,
        config: SeparationConfig,
        stage_inputs: Mapping[str, str],
    ) -> dict[str, Any] | None:
        version_path = self.workspace.slice_dir(slice_id) / "version.json"
        if not version_path.is_file():
            return None
        try:
            previous = read_json(version_path)
        except (OSError, ValueError):
            return None
        previous_inputs = previous.get("stage_inputs")
        if not isinstance(previous_inputs, Mapping):
            return None
        earliest = None
        for stage in STAGE_ORDER:
            if previous_inputs.get(stage) != stage_inputs[stage]:
                earliest = stage
                break
        if earliest is None:
            return None
        previous_sources = _sources_from_payload(previous.get("sources"))
        if earliest == "mask_init" and previous_sources:
            affected = changed_bands_for_sources(previous_sources, slice_truth.sources, config)
            if not affected:
                affected = list(range(config.band_count))
        else:
            affected = list(range(config.band_count))
        reasons = {
            "stft": "slice_content_or_stft_config_changed",
            "features": "band_partition_or_feature_config_changed",
            "mask_init": "reference_f0_or_source_truth_changed",
            "mask_refine": "mask_iteration_count_changed",
            "reconstruct": "overlap_or_reconstruction_config_changed",
        }
        record = self._append_invalidation(
            slice_id=slice_id,
            reason=reasons[earliest],
            earliest_stage=earliest,
            affected_bands=[int(value) for value in affected],
            from_fingerprint=digest_json(previous_inputs),
            to_fingerprint=digest_json(stage_inputs),
        )
        return record

    def _append_invalidation(
        self,
        *,
        slice_id: str,
        reason: str,
        earliest_stage: str,
        affected_bands: Sequence[int],
        from_fingerprint: str | None,
        to_fingerprint: str,
    ) -> dict[str, Any]:
        path = self.workspace.slice_dir(slice_id) / "invalidations.json"
        try:
            records = read_json(path) if path.is_file() else []
        except (OSError, ValueError):
            records = []
        signature = {
            "reason": reason,
            "earliest_stage": earliest_stage,
            "affected_bands": [int(value) for value in sorted(set(affected_bands))],
            "from_fingerprint": from_fingerprint,
            "to_fingerprint": to_fingerprint,
        }
        for existing in records:
            if all(existing.get(key) == value for key, value in signature.items()):
                return existing
        ranges: list[list[int]] = []
        for band in signature["affected_bands"]:
            if ranges and band == ranges[-1][1] + 1:
                ranges[-1][1] = band
            else:
                ranges.append([band, band])
        record = {
            "sequence": len(records) + 1,
            **signature,
            "affected_band_ranges": ranges,
        }
        records.append(record)
        atomic_write_json(path, records)
        return record

    def _write_version(
        self,
        *,
        slice_id: str,
        manifest: AudioManifest,
        config: SeparationConfig,
        stage_inputs: Mapping[str, str],
        products: Mapping[str, str],
        sources: Sequence[SourceTruth] | None = None,
    ) -> None:
        truth = manifest.get(slice_id)
        version = {
            "schema_version": 1,
            "slice_id": slice_id,
            "config_digest": digest_json(config.to_dict()),
            "manifest_digest": manifest.digest(),
            "overrides_digest": digest_json(self.workspace.raw_overrides()),
            "config": config.to_dict(),
            "sources": _source_payload(sources if sources is not None else truth.sources),
            "stage_inputs": dict(stage_inputs),
            "stage_products": dict(products),
            "version_fingerprint": digest_json({"inputs": stage_inputs, "products": products}),
        }
        atomic_write_json(self.workspace.slice_dir(slice_id) / "version.json", version)

    def _current_version_fingerprint(self, slice_id: str) -> str | None:
        path = self.workspace.slice_dir(slice_id) / "version.json"
        if not path.is_file():
            return None
        try:
            record = read_json(path)
        except (OSError, ValueError):
            return None
        return record.get("version_fingerprint")

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    def _config_for_slice(self, slice_id: str) -> SeparationConfig:
        try:
            return self.workspace.config()
        except AppError as exc:
            if exc.code == "E_INVALID_CONFIG" and "slice_id" not in exc.details:
                exc.details["slice_id"] = slice_id
            raise

    def _cache_meta(
        self,
        *,
        stage: str,
        input_fp: str,
        config: SeparationConfig,
        completed: set[int],
        block_digests: Mapping[str, str],
        extra: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        meta = {
            "schema_version": 1,
            "stage": stage,
            "input_fingerprint": input_fp,
            "config_version": config.fingerprint(),
            "completed_blocks": sorted(int(value) for value in completed),
            "block_digests": {str(key): str(value) for key, value in block_digests.items()},
        }
        if extra:
            meta.update(json_safe(extra))
        return meta

    @staticmethod
    def _stft_full_digest(arrays: Mapping[str, np.ndarray]) -> str:
        return digest_json(
            {
                "spectra": digest_array(arrays["spectra"]),
                "starts": digest_array(arrays["frame_starts"]),
                "valid_lengths": digest_array(arrays["valid_lengths"]),
                "padded": digest_array(arrays["padded"]),
            }
        )

    @staticmethod
    def _reconstruction_plan_digest(
        recon_blocks: Sequence[ReconstructionBlock],
        slice_truth: SliceTruth,
        config: SeparationConfig,
    ) -> str:
        return digest_json(
            {
                "blocks": [block.to_dict() for block in recon_blocks],
                "frame_length": config.frame_length,
                "hop_length": config.hop_length,
                "block_frames": config.block_frames,
                "block_overlap_percent": config.block_overlap_percent,
                "n_samples": slice_truth.n_samples,
                "source_count": slice_truth.source_count,
            }
        )

    def _load_complete_caches(
        self,
        slice_id: str,
        slice_truth: SliceTruth,
        config: SeparationConfig,
        audio: np.ndarray,
        process_blocks: Sequence[ProcessBlock],
    ) -> dict[str, Any]:
        manifest = self.workspace.effective_manifest()
        stage_inputs = self._stage_inputs(slice_truth, config, manifest)
        cache_dir = self.workspace.cache_dir(slice_id)
        n_frames = frame_count(slice_truth.n_samples, config.hop_length)
        stft_loaded = _load_optional_npz(cache_dir / "stft.npz")
        if not stft_loaded:
            raise conflict("STFT stage cache is missing; run build first", slice_id=slice_id)
        stft_arrays, stft_meta = stft_loaded
        if stft_meta.get("input_fingerprint") != stage_inputs["stft"]:
            raise conflict("STFT stage cache is stale; run build first", slice_id=slice_id)
        stft = StftResult(
            spectra=stft_arrays["spectra"],
            frame_indices=stft_arrays["frame_indices"],
            frame_starts=stft_arrays["frame_starts"],
            valid_lengths=stft_arrays["valid_lengths"],
            padded=stft_arrays["padded"],
            window=np.hanning(config.frame_length).astype(np.float64),
            n_samples=slice_truth.n_samples,
            digest=self._stft_full_digest(stft_arrays),
        )
        features_loaded = _load_optional_npz(cache_dir / "features.npz")
        if not features_loaded or features_loaded[1].get("input_fingerprint") != stage_inputs["features"]:
            raise conflict("feature stage cache is missing or stale; run build first", slice_id=slice_id)
        features_arrays = features_loaded[0]
        features = BandFeatures(
            features=features_arrays["features"],
            band_edges_hz=features_arrays["edge_hz"],
            bin_band=features_arrays["bin_band"],
            digest=feature_digest(features_arrays["features"], features_arrays["edge_hz"], features_arrays["bin_band"]),
        )
        init_loaded = _load_optional_npz(cache_dir / "masks_init.npz")
        if not init_loaded or init_loaded[1].get("input_fingerprint") != stage_inputs["mask_init"]:
            raise conflict("mask initialization cache is missing or stale; run build first", slice_id=slice_id)
        refine_loaded = _load_optional_npz(cache_dir / "masks_refine.npz")
        if not refine_loaded or refine_loaded[1].get("input_fingerprint") != stage_inputs["mask_refine"]:
            raise conflict("mask refinement cache is missing or stale; run build first", slice_id=slice_id)
        refine_arrays = refine_loaded[0]
        if "masks" not in refine_arrays or "round_changes" not in refine_arrays or "rounds_done" not in refine_arrays:
            raise conflict("mask refinement cache is incomplete; run build first", slice_id=slice_id)
        if np.any(refine_arrays["rounds_done"] < config.mask_iterations):
            raise conflict("mask refinement is not complete; run build/resume first", slice_id=slice_id)
        recon_blocks = make_reconstruction_blocks(process_blocks, n_frames, slice_truth.n_samples, config)
        for block in recon_blocks:
            path = self.workspace.recon_dir(slice_id) / f"b{block.index:06d}.npz"
            loaded = _load_optional_npz(path)
            if not loaded:
                raise conflict("reconstruction cache is incomplete; run build first", slice_id=slice_id, block_index=block.index)
            if loaded[1].get("input_fingerprint") != stage_inputs["reconstruct"]:
                raise conflict("reconstruction cache is stale; run build first", slice_id=slice_id, block_index=block.index)
        return {
            "stft": stft,
            "features": features,
            "masks_init": init_loaded[0]["masks"],
            "masks": refine_arrays["masks"],
            "round_changes": refine_arrays["round_changes"],
            "rounds_done": refine_arrays["rounds_done"],
            "recon_blocks": recon_blocks,
            "process_blocks": process_blocks,
            "norm": compute_ola_norm(recon_blocks, slice_truth.n_samples, config),
        }

