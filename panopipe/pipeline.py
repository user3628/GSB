"""Four-stage panoptic build pipeline with resumable checkpoints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from .checkpoints import STAGE_INDEX, STAGES
from .codecs import (
    decode_coco_json,
    decode_png,
    decode_rle,
    encode_coco_json,
    encode_png,
    encode_rle,
    label_maps_equal,
    rle_from_json,
    rle_to_json,
    segment_tables_equal,
)
from .errors import (
    E_BUSY,
    E_INVALID_CONFIG,
    E_INVALID_STAGE,
    E_NOT_FOUND,
    E_STALE_CHECKPOINT,
    PipelineError,
)
from .fingerprint import digest_file, digest_files, digest_json
from .fsutil import atomic_write_bytes, atomic_write_json, atomic_write_text, file_lock
from .manifest import Manifest, ShardSpec
from .model import PanopticModel, SegmentInfo
from .semantic import DecodeResult, decode_semantic, merge_instances
from .workspace import RuntimeConfig, Workspace


FORMAT_FILE_NAMES = {
    "png_palette": "mask.png",
    "rle": "mask.rle.txt",
    "coco_json": "mask.coco.json",
}


def _stage_or_error(stage: str) -> str:
    if stage not in STAGE_INDEX:
        raise PipelineError(
            E_INVALID_STAGE,
            "Unknown pipeline stage",
            {"stage": stage, "supported": list(STAGES)},
        )
    return stage


def _normalize_stages(stages: Iterable[str] | None) -> list[str] | None:
    if stages is None:
        return None
    requested = [_stage_or_error(str(stage)) for stage in stages]
    if not requested:
        raise PipelineError(E_INVALID_STAGE, "At least one stage must be requested")
    if len(set(requested)) != len(requested):
        raise PipelineError(E_INVALID_STAGE, "Requested stages must be unique")
    indices = [STAGE_INDEX[stage] for stage in requested]
    if indices != sorted(indices) or indices != list(range(indices[0], indices[0] + len(indices))):
        raise PipelineError(
            E_INVALID_STAGE,
            "Requested stages must form a contiguous forward sequence",
            {"requested": requested, "pipeline_order": list(STAGES)},
        )
    return requested


class Pipeline:
    def __init__(self, workspace: Workspace):
        self.workspace = workspace
        self.checkpoints = workspace.checkpoints

    def _load(self) -> tuple[Manifest, RuntimeConfig]:
        self.workspace.require_imported()
        manifest = self.workspace.load_manifest()
        config = self.workspace.load_config()
        return manifest, config

    def _source_fingerprint(self, shard: ShardSpec) -> str:
        try:
            return digest_file(shard.image_path)
        except FileNotFoundError as exc:
            raise PipelineError(
                E_NOT_FOUND,
                "Shard input image is missing",
                {"shard_id": shard.shard_id, "path": shard.image_ref},
            ) from exc

    def _axes(self, manifest: Manifest, shard: ShardSpec, config: RuntimeConfig, source_fp: str) -> dict[str, Any]:
        version_axes = manifest.version_axes(shard)
        return {
            **version_axes,
            "source_fingerprint": source_fp,
            "codec_fingerprint": config.fingerprint(),
            "palette_max_colors": config.palette_max_colors,
            "segment_id_bits": config.segment_id_bits,
            "png_compress_level": config.png_compress_level,
            "export_formats": list(config.export_formats),
            "validate_formats": list(config.validate_formats),
        }

    def _expected(self, axes: Mapping[str, Any]) -> dict[str, Any]:
        decode = digest_json(
            {
                "stage": "decode",
                "schema_version": axes["schema_version"],
                "source_fingerprint": axes["source_fingerprint"],
                "category_fingerprint": axes["category_fingerprint"],
                "source_encoding": axes["source_encoding"],
            }
        )
        merge = digest_json(
            {
                "stage": "merge",
                "upstream": decode,
                "schema_version": axes["schema_version"],
                "semantics_fingerprint": axes["semantics_fingerprint"],
                "instance_start_id": axes["instance_start_id"],
                "min_instance_area": axes["min_instance_area"],
                "confidence_threshold": axes["confidence_threshold"],
            }
        )
        encode = digest_json(
            {
                "stage": "encode",
                "upstream": merge,
                "mask_format": axes["mask_format"],
                "codec_fingerprint": axes["codec_fingerprint"],
                "export_formats": axes["export_formats"],
            }
        )
        validate = digest_json(
            {
                "stage": "validate",
                "upstream": encode,
                "validate_formats": axes["validate_formats"],
                "codec_fingerprint": axes["codec_fingerprint"],
            }
        )
        return {"decode": decode, "merge": merge, "encode": encode, "validate": validate}

    def _manifest_fp(self, manifest: Manifest, shard: ShardSpec) -> str:
        return manifest.manifest_version_fingerprint(shard)

    def _record_artifacts_valid(self, record: Mapping[str, Any]) -> bool:
        files = record.get("artifact_files")
        if not isinstance(files, list) or not files:
            return False
        paths: list[str] = []
        for entry in files:
            if not isinstance(entry, Mapping) or not isinstance(entry.get("path"), str):
                return False
            rel = entry["path"]
            path = Path(rel)
            if path.is_absolute() or ".." in path.parts:
                return False
            if not (self.workspace.root / path).is_file():
                return False
            paths.append(rel)
        try:
            digest, _ = digest_files(self.workspace.root, paths)
        except FileNotFoundError:
            return False
        return digest == record.get("artifact_digest")

    def _latest_checkpoint(self, shard_id: str, stage: str) -> dict[str, Any] | None:
        return self.checkpoints.latest(shard_id, stage)

    def _find_valid_checkpoint(self, shard_id: str, stage: str, expected_fp: str) -> dict[str, Any] | None:
        candidates = self.checkpoints.records_for(shard_id, stage)
        for record in reversed(candidates):
            if record.get("stage_fingerprint") != expected_fp:
                continue
            if self._record_artifacts_valid(record):
                return record
        return None

    def _status_for_shard(
        self,
        manifest: Manifest,
        shard: ShardSpec,
        config: RuntimeConfig,
    ) -> dict[str, Any]:
        source_fp = self._source_fingerprint(shard)
        axes = self._axes(manifest, shard, config, source_fp)
        expected = self._expected(axes)
        manifest_fp = self._manifest_fp(manifest, shard)
        stages: dict[str, Any] = {}
        pending_changes: list[dict[str, Any]] = []
        stale_stages: list[str] = []
        for stage in STAGES:
            latest = self._latest_checkpoint(shard.shard_id, stage)
            valid = self._find_valid_checkpoint(shard.shard_id, stage, expected[stage])
            if valid is not None:
                state = "ready"
            elif latest is None:
                state = "missing"
            elif latest.get("stage_fingerprint") != expected[stage]:
                state = "stale"
                stale_stages.append(stage)
            else:
                state = "artifact_missing"
                stale_stages.append(stage)
            stages[stage] = {
                "status": state,
                "checkpoint_sequence": int(latest["sequence"]) if latest else None,
                "checkpoint_fingerprint": latest.get("stage_fingerprint") if latest else None,
                "current_fingerprint": expected[stage],
                "artifact_digest": latest.get("artifact_digest") if valid else None,
                "artifact_present": valid is not None,
            }
        latest_records = [record for record in (self._latest_checkpoint(shard.shard_id, s) for s in STAGES) if record]
        old_axes: Mapping[str, Any] = {}
        if latest_records:
            old_axes = max(latest_records, key=lambda item: int(item["sequence"])).get("configuration_axes", {})
        if stale_stages:
            pending_changes = self._change_reasons(old_axes, axes)
        return {
            "shard_id": shard.shard_id,
            "image": shard.image_ref,
            "source_fingerprint": source_fp,
            "manifest_version_fingerprint": manifest_fp,
            "axes": axes,
            "fingerprints": expected,
            "stages": stages,
            "pending_invalidation": {
                "active": bool(stale_stages),
                "earliest_stage": stale_stages[0] if stale_stages else None,
                "invalidated_stages": stale_stages,
                "reasons": pending_changes,
            },
        }

    def _change_reasons(self, previous: Mapping[str, Any], current: Mapping[str, Any]) -> list[dict[str, Any]]:
        checks = [
            ("schema_version", "schema_version_changed", "decode"),
            ("source_fingerprint", "source_pixel_content_changed", "decode"),
            ("category_fingerprint", "category_mapping_changed", "decode"),
            ("instance_start_id", "instance_start_id_changed", "merge"),
            ("min_instance_area", "area_threshold_changed", "merge"),
            ("confidence_threshold", "confidence_threshold_changed", "merge"),
            ("semantics_fingerprint", "thing_or_category_confidence_changed", "merge"),
            ("mask_format", "mask_format_preference_changed", "encode"),
            ("codec_fingerprint", "codec_configuration_changed", "encode"),
            ("export_formats", "export_format_set_changed", "encode"),
            ("validate_formats", "validation_configuration_changed", "validate"),
        ]
        reasons: list[dict[str, Any]] = []
        for field, code, stage in checks:
            if previous.get(field) != current.get(field):
                reasons.append(
                    {
                        "code": code,
                        "stage": stage,
                        "previous": previous.get(field),
                        "current": current.get(field),
                    }
                )
        if not reasons:
            reasons.append(
                {
                    "code": "artifact_missing_or_unreadable",
                    "stage": "decode",
                    "previous": None,
                    "current": None,
                }
            )
        return reasons

    def status(self, *, shard_ids: Iterable[str] | None = None) -> dict[str, Any]:
        manifest, config = self._load()
        selected = self._select_shards(manifest, shard_ids)
        shard_status = {
            shard.shard_id: self._status_for_shard(manifest, shard, config) for shard in selected
        }
        invalidations = self.workspace.load_invalidations()
        return {
            "ok": True,
            "workspace_imported": True,
            "project": manifest.project,
            "schema_version": manifest.schema_version,
            "overall_manifest_fingerprint": manifest.overall_fingerprint(),
            "checkpoint_sequence": self.checkpoints.last_sequence(),
            "stop_requested": self.workspace.stop_requested(),
            "config": config.to_dict(),
            "shards": shard_status,
            "invalidations": invalidations.get("shards", {}),
        }

    def fingerprints(self) -> dict[str, Any]:
        manifest, config = self._load()
        result: dict[str, Any] = {
            "ok": True,
            "overall_manifest_fingerprint": manifest.overall_fingerprint(),
            "schema_version": manifest.schema_version,
            "config_fingerprint": config.fingerprint(),
            "shards": {},
        }
        for shard in manifest.shards:
            source_fp = self._source_fingerprint(shard)
            axes = self._axes(manifest, shard, config, source_fp)
            result["shards"][shard.shard_id] = {
                "axes": axes,
                "manifest_version_fingerprint": self._manifest_fp(manifest, shard),
                "stage_fingerprints": self._expected(axes),
            }
        return result

    def _select_shards(self, manifest: Manifest, shard_ids: Iterable[str] | None) -> list[ShardSpec]:
        if shard_ids is None:
            return list(manifest.shards)
        ids = [str(item) for item in shard_ids]
        unknown = sorted(set(ids) - set(manifest.shard_by_id))
        if unknown:
            raise PipelineError(E_NOT_FOUND, "Unknown shard ids", {"shard_ids": unknown})
        return [manifest.shard_by_id[shard_id] for shard_id in ids]

    def _validate_request(
        self,
        manifest: Manifest,
        config: RuntimeConfig,
        selected: list[ShardSpec],
        stages: list[str] | None,
    ) -> None:
        # Full builds may rebuild prerequisites. Explicit stage requests may not
        # silently jump over a missing or stale prerequisite.
        if stages is None:
            return
        first_index = STAGE_INDEX[stages[0]]
        for shard in selected:
            source_fp = self._source_fingerprint(shard)
            axes = self._axes(manifest, shard, config, source_fp)
            expected = self._expected(axes)
            for prerequisite in STAGES[:first_index]:
                if self._find_valid_checkpoint(shard.shard_id, prerequisite, expected[prerequisite]) is None:
                    raise PipelineError(
                        E_INVALID_STAGE,
                        "Requested stage requires a current prerequisite artifact",
                        {
                            "shard_id": shard.shard_id,
                            "requested_stage": stages[0],
                            "missing_prerequisite": prerequisite,
                        },
                    )

    def _apply_invalidations(
        self,
        manifest: Manifest,
        config: RuntimeConfig,
        selected: list[ShardSpec],
    ) -> dict[str, Any]:
        state = self.workspace.load_invalidations()
        shard_events: dict[str, Any] = dict(state.get("shards", {}))
        any_change = False
        for shard in selected:
            source_fp = self._source_fingerprint(shard)
            axes = self._axes(manifest, shard, config, source_fp)
            expected = self._expected(axes)
            latest_per_stage = {stage: self._latest_checkpoint(shard.shard_id, stage) for stage in STAGES}
            stale: list[str] = []
            for stage in STAGES:
                latest = latest_per_stage[stage]
                if latest is None:
                    continue
                if latest.get("stage_fingerprint") != expected[stage] or not self._record_artifacts_valid(latest):
                    stale.append(stage)
            if not stale:
                if (
                    shard.shard_id in shard_events
                    and shard_events[shard.shard_id].get("active")
                    and self._all_stages_current(manifest, config, shard)
                ):
                    shard_events[shard.shard_id]["active"] = False
                    shard_events[shard.shard_id]["resolved"] = True
                    shard_events[shard.shard_id]["resolved_at_sequence"] = self.checkpoints.last_sequence()
                    any_change = True
                continue
            earliest = min(stale, key=lambda item: STAGE_INDEX[item])
            latest_records = [record for record in latest_per_stage.values() if record]
            old_axes = max(latest_records, key=lambda item: int(item["sequence"])).get("configuration_axes", {})
            reasons = self._change_reasons(old_axes, axes)
            invalidated = list(STAGES[STAGE_INDEX[earliest] :])
            self.workspace.remove_stage_artifacts(shard.shard_id, invalidated)
            shard_events[shard.shard_id] = {
                "active": True,
                "resolved": False,
                "earliest_stage": earliest,
                "invalidated_stages": invalidated,
                "reasons": reasons,
                "previous_sequence": self.checkpoints.last_sequence(),
                "expected_fingerprints": expected,
                "manifest_version_fingerprint": self._manifest_fp(manifest, shard),
            }
            any_change = True
        if any_change:
            self.workspace.write_invalidations(
                {
                    "format": "PANO_INVALIDATIONS_V1",
                    "shards": {key: shard_events[key] for key in sorted(shard_events)},
                }
            )
        return shard_events

    def _artifact_digest(self, shard_id: str, stage: str) -> tuple[str, list[dict[str, str]]]:
        paths = self.workspace.artifact_paths(shard_id, stage)
        if not paths:
            raise PipelineError(
                E_INVALID_STAGE,
                "Stage produced no artifacts",
                {"shard_id": shard_id, "stage": stage},
            )
        return digest_files(self.workspace.root, paths)

    def _publish_stage(
        self,
        *,
        shard: ShardSpec,
        stage: str,
        input_fingerprint: str,
        manifest_fp: str,
        stage_fp: str,
        axes: Mapping[str, Any],
    ) -> dict[str, Any]:
        artifact_digest, artifact_files = self._artifact_digest(shard.shard_id, stage)
        record = {
            "shard_id": shard.shard_id,
            "stage": stage,
            "input_fingerprint": input_fingerprint,
            "manifest_version_fingerprint": manifest_fp,
            "artifact_digest": artifact_digest,
            "stage_fingerprint": stage_fp,
            "artifact_files": artifact_files,
            "configuration_axes": dict(axes),
            "pipeline_format": "PANO_CHECKPOINT_V1",
        }
        try:
            return self.checkpoints.publish(record)
        except PipelineError as exc:
            if exc.code != E_STALE_CHECKPOINT:
                raise
            # If another worker won the race with the same stage, adopt that
            # checkpoint rather than producing a duplicate record.
            valid = self._find_valid_checkpoint(shard.shard_id, stage, stage_fp)
            if valid is not None:
                return valid
            raise

    def _load_decode(self, shard: ShardSpec) -> DecodeResult:
        directory = self.workspace.shard_dir(shard.shard_id, "decode")
        labels = self.workspace.load_npy(directory / "labels.npy")
        try:
            meta = json.loads((directory / "meta.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineError(E_INVALID_STAGE, "Decode artifact metadata is missing or invalid") from exc
        return DecodeResult(
            shard_id=str(meta["shard_id"]),
            width=int(meta["width"]),
            height=int(meta["height"]),
            category_ids=labels,
            source_encoding=str(meta["source_encoding"]),
            source_fingerprint=str(meta["source_fingerprint"]),
        )

    def _load_model(self, shard: ShardSpec) -> PanopticModel:
        directory = self.workspace.shard_dir(shard.shard_id, "merge")
        labels = self.workspace.load_npy(directory / "labels.npy")
        try:
            meta = json.loads((directory / "model.json").read_text(encoding="utf-8"))
            segments = [SegmentInfo.from_dict(item) for item in meta["segments"]]
            width = int(meta["width"])
            height = int(meta["height"])
            shard_id = str(meta["shard_id"])
        except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
            raise PipelineError(E_INVALID_STAGE, "Merge artifact model is missing or invalid") from exc
        return PanopticModel(shard_id=shard_id, width=width, height=height, labels=labels, segments=segments)

    def _run_decode_stage(self, shard: ShardSpec) -> None:
        try:
            decoded = decode_semantic(shard.image_path, shard)
        except PipelineError as exc:
            if exc.code.startswith("E_"):
                exc.details.setdefault("shard_id", shard.shard_id)
                self.workspace.write_diagnostic(shard.shard_id, exc)
            raise
        directory = self.workspace.shard_dir(shard.shard_id, "decode")
        self.workspace.save_npy(directory / "labels.npy", decoded.category_ids)
        atomic_write_json(directory / "meta.json", decoded.to_meta(shard))

    def _run_merge_stage(self, shard: ShardSpec) -> None:
        decoded = self._load_decode(shard)
        model = merge_instances(decoded, shard)
        directory = self.workspace.shard_dir(shard.shard_id, "merge")
        self.workspace.save_npy(directory / "labels.npy", model.labels)
        value = model.to_dict()
        value["model_digest"] = model.digest()
        atomic_write_json(directory / "model.json", value)

    def _run_encode_stage(
        self,
        shard: ShardSpec,
        config: RuntimeConfig,
    ) -> dict[str, Any]:
        model = self._load_model(shard)
        options = config.codec_options()
        directory = self.workspace.shard_dir(shard.shard_id, "encode")
        directory.mkdir(parents=True, exist_ok=True)
        produced: list[str] = []
        degradations: list[dict[str, Any]] = []
        errors_by_format: dict[str, str] = {}
        formats = list(config.export_formats)
        if shard.mask_format not in formats:
            formats.append(shard.mask_format)
        for format_name in formats:
            try:
                if format_name == "png_palette":
                    atomic_write_bytes(directory / FORMAT_FILE_NAMES[format_name], encode_png(model, options))
                elif format_name == "rle":
                    atomic_write_text(directory / FORMAT_FILE_NAMES[format_name], encode_rle(model, options))
                elif format_name == "coco_json":
                    atomic_write_text(directory / FORMAT_FILE_NAMES[format_name], encode_coco_json(model, options))
                else:
                    raise PipelineError(E_INVALID_CONFIG, "Unknown export format", {"format": format_name})
                produced.append(format_name)
            except PipelineError as exc:
                if exc.code not in {"E_PALETTE_CAPACITY", "E_SEGMENT_ID_OVERFLOW"}:
                    raise
                degradation = dict(exc.details.get("degradation", {}))
                degradation.setdefault("code", f"D_{exc.code.removeprefix('E_')}")
                degradation.setdefault("error_code", exc.code)
                degradation.setdefault("format", format_name)
                degradation.setdefault("action", "format_not_encoded")
                degradation.setdefault("lossless", False)
                degradations.append(degradation)
                errors_by_format[format_name] = exc.code
        preferred_available = shard.mask_format in produced
        fallback = produced[0] if produced else None
        if not preferred_available:
            degradations.append(
                {
                    "code": "D_FORMAT_FALLBACK",
                    "error_code": errors_by_format.get(shard.mask_format, "E_FORMAT_MISSING"),
                    "format": shard.mask_format,
                    "fallback": fallback,
                    "action": "used_fallback" if fallback else "no_lossless_format_available",
                    "lossless": False,
                }
            )
        meta = {
            "format": "PANO_ENCODE_META_V1",
            "shard_id": shard.shard_id,
            "model_digest": model.digest(),
            "preferred_format": shard.mask_format,
            "preferred_available": preferred_available,
            "fallback_format": fallback,
            "produced_formats": produced,
            "degradations": degradations,
            "format_errors": errors_by_format,
        }
        atomic_write_json(directory / "encode.meta.json", meta)
        atomic_write_json(directory / "degradations.json", degradations)
        return meta

    def _run_validate_stage(self, shard: ShardSpec, config: RuntimeConfig) -> dict[str, Any]:
        model = self._load_model(shard)
        directory = self.workspace.shard_dir(shard.shard_id, "encode")
        try:
            encode_meta = json.loads((directory / "encode.meta.json").read_text(encoding="utf-8"))
            degradations = json.loads((directory / "degradations.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineError(E_INVALID_STAGE, "Encode metadata is missing or invalid") from exc
        error_by_format = {
            item.get("format"): item.get("error_code")
            for item in degradations
            if isinstance(item, Mapping) and item.get("format")
        }
        chains: list[dict[str, Any]] = []
        for format_name in config.validate_formats:
            chain_name = "rle_json" if format_name == "rle" else format_name
            chain: dict[str, Any] = {
                "format": format_name,
                "chain": chain_name,
                "ok": False,
                "labels_equal": False,
                "segments_equal": False,
                "pixel_count_sum_ok": False,
                "error": None,
            }
            file_path = directory / FORMAT_FILE_NAMES[format_name]
            if not file_path.is_file():
                chain["error"] = {
                    "code": error_by_format.get(format_name, "E_FORMAT_MISSING"),
                    "message": "Round-trip format was not produced",
                }
                chains.append(chain)
                continue
            try:
                if format_name == "png_palette":
                    decoded = decode_png(file_path.read_bytes(), config.codec_options())
                elif format_name == "rle":
                    rle_value = file_path.read_text(encoding="utf-8")
                    rle_value = rle_from_json(rle_to_json(rle_value))
                    decoded = decode_rle(rle_value, config.codec_options())
                elif format_name == "coco_json":
                    decoded = decode_coco_json(file_path.read_text(encoding="utf-8"), config.codec_options())
                else:
                    raise PipelineError(E_INVALID_CONFIG, "Unknown validation format")
                labels_equal = label_maps_equal(model, decoded)
                segments_equal = segment_tables_equal(model, decoded)
                pixel_count_sum = sum(segment.pixel_count for segment in decoded.segments)
                pixel_count_sum_ok = pixel_count_sum == model.width * model.height
                chain.update(
                    {
                        "ok": labels_equal and segments_equal and pixel_count_sum_ok,
                        "labels_equal": labels_equal,
                        "segments_equal": segments_equal,
                        "pixel_count_sum_ok": pixel_count_sum_ok,
                        "decoded_segment_count": len(decoded.segments),
                        "decoded_pixel_count": pixel_count_sum,
                        "decoded_model_digest": decoded.digest(),
                    }
                )
            except PipelineError as exc:
                chain["error"] = {"code": exc.code, "message": exc.message, "details": exc.details}
            chains.append(chain)
        all_ok = all(chain["ok"] for chain in chains)
        error_codes = sorted(
            {
                chain["error"]["code"]
                for chain in chains
                if isinstance(chain.get("error"), Mapping) and chain["error"].get("code")
            }
        )
        validation = {
            "format": "PANO_VALIDATION_V1",
            "shard_id": shard.shard_id,
            "ok": all_ok,
            "degraded": bool(degradations),
            "model_digest": model.digest(),
            "label_shape": [model.height, model.width],
            "segment_count": len(model.segments),
            "pixel_count": model.pixel_count,
            "pixel_count_sum": sum(segment.pixel_count for segment in model.segments),
            "chains": chains,
            "degradations": degradations,
            "error_codes": error_codes,
            "preferred_format": shard.mask_format,
            "preferred_available": bool(encode_meta.get("preferred_available")),
            "summary": {
                "chain_count": len(chains),
                "passed": sum(1 for chain in chains if chain["ok"]),
                "degradation_count": len(degradations),
            },
        }
        directory = self.workspace.shard_dir(shard.shard_id, "validate")
        atomic_write_json(directory / "validation.json", validation)
        return validation

    def build(
        self,
        *,
        shard_ids: Iterable[str] | None = None,
        stages: Iterable[str] | None = None,
        resume: bool = False,
        stop_after_shards: int | None = None,
    ) -> dict[str, Any]:
        manifest, config = self._load()
        requested_stages = _normalize_stages(stages)
        selected = self._select_shards(manifest, shard_ids)
        self._validate_request(manifest, config, selected, requested_stages)
        if resume:
            self.workspace.clear_stop()
        if self.workspace.stop_requested():
            return {
                "ok": False,
                "stopped": True,
                "error": {
                    "code": "E_STOP_REQUESTED",
                    "message": "A stop was requested; use resume to clear it",
                },
                "completed_shards": [],
                "reused_stages": [],
                "rebuilt_stages": [],
                "checkpoint_sequence": self.checkpoints.last_sequence(),
            }
        if stop_after_shards is not None and stop_after_shards < 0:
            raise PipelineError(E_INVALID_CONFIG, "stop_after_shards cannot be negative")

        try:
            lock_context = file_lock(self.workspace.build_lock_path, blocking=False)
            lock_context.__enter__()
        except BlockingIOError as exc:
            raise PipelineError(
                E_BUSY,
                "Another build is active for this workspace",
                {"workspace": str(self.workspace.root)},
            ) from exc
        try:
            invalidations = self._apply_invalidations(manifest, config, selected)
            completed: list[str] = []
            reused: list[dict[str, Any]] = []
            rebuilt: list[dict[str, Any]] = []
            shard_results: dict[str, Any] = {}
            stopped = False
            for index, shard in enumerate(selected):
                if index and self.workspace.stop_requested():
                    stopped = True
                    break
                if stop_after_shards is not None and index >= stop_after_shards:
                    stopped = True
                    break
                result = self._build_shard(manifest, config, shard, requested_stages)
                shard_results[shard.shard_id] = result
                reused.extend(result["reused_stages"])
                rebuilt.extend(result["rebuilt_stages"])
                completed.append(shard.shard_id)
            # Persist invalidation resolution only after a complete selected run.
            if not stopped:
                state = self.workspace.load_invalidations()
                events = dict(state.get("shards", {}))
                changed = False
                for shard in selected:
                    event = events.get(shard.shard_id)
                    if (
                        isinstance(event, dict)
                        and event.get("active")
                        and self._all_stages_current(manifest, config, shard)
                    ):
                        event["active"] = False
                        event["resolved"] = True
                        event["resolved_at_sequence"] = self.checkpoints.last_sequence()
                        changed = True
                if changed:
                    self.workspace.write_invalidations(
                        {"format": "PANO_INVALIDATIONS_V1", "shards": {key: events[key] for key in sorted(events)}}
                    )
            summary_digest = self._summary_digest(shard_results)
            includes_validation = requested_stages is None or "validate" in requested_stages
            if includes_validation:
                all_validations_ok = all(
                    result.get("validation", {}).get("ok", False) for result in shard_results.values()
                ) and bool(shard_results) and not stopped
            else:
                all_validations_ok = bool(shard_results) and not stopped
            return {
                "ok": all_validations_ok,
                "stopped": stopped,
                "project": manifest.project,
                "completed_shards": completed,
                "reused_stages": reused,
                "rebuilt_stages": rebuilt,
                "shards": shard_results,
                "summary_digest": summary_digest,
                "checkpoint_sequence": self.checkpoints.last_sequence(),
                "invalidations": {shard.shard_id: invalidations.get(shard.shard_id) for shard in selected},
            }
        finally:
            lock_context.__exit__(None, None, None)

    def _build_shard(
        self,
        manifest: Manifest,
        config: RuntimeConfig,
        shard: ShardSpec,
        requested_stages: list[str] | None,
    ) -> dict[str, Any]:
        source_fp = self._source_fingerprint(shard)
        axes = self._axes(manifest, shard, config, source_fp)
        expected = self._expected(axes)
        manifest_fp = self._manifest_fp(manifest, shard)
        stages = requested_stages or list(STAGES)
        reused: list[dict[str, Any]] = []
        rebuilt: list[dict[str, Any]] = []
        stage_records: dict[str, dict[str, Any]] = {}
        validation: dict[str, Any] | None = None
        for stage in stages:
            current = self._find_valid_checkpoint(shard.shard_id, stage, expected[stage])
            if current is not None:
                stage_records[stage] = current
                reused.append(
                    {"shard_id": shard.shard_id, "stage": stage, "sequence": current["sequence"], "reason": "fingerprint_match"}
                )
                if stage == "validate":
                    validation = self._read_validation(shard)
                continue
            # For an explicit later-stage request, _validate_request already
            # proved prerequisites were current before this build started.
            if stage == "decode":
                self._run_decode_stage(shard)
                input_fp = source_fp
            elif stage == "merge":
                decode_record = stage_records.get("decode") or self._requirement(shard, "decode", expected)
                self._run_merge_stage(shard)
                input_fp = decode_record["artifact_digest"]
            elif stage == "encode":
                merge_record = stage_records.get("merge") or self._requirement(shard, "merge", expected)
                self._run_encode_stage(shard, config)
                input_fp = merge_record["artifact_digest"]
            elif stage == "validate":
                encode_record = stage_records.get("encode") or self._requirement(shard, "encode", expected)
                validation = self._run_validate_stage(shard, config)
                input_fp = encode_record["artifact_digest"]
            else:
                raise PipelineError(E_INVALID_STAGE, "Unknown stage", {"stage": stage})
            record = self._publish_stage(
                shard=shard,
                stage=stage,
                input_fingerprint=input_fp,
                manifest_fp=manifest_fp,
                stage_fp=expected[stage],
                axes=axes,
            )
            stage_records[stage] = record
            rebuilt.append(
                {
                    "shard_id": shard.shard_id,
                    "stage": stage,
                    "sequence": record["sequence"],
                    "artifact_digest": record["artifact_digest"],
                }
            )
        if validation is None and self._find_valid_checkpoint(shard.shard_id, "validate", expected["validate"]):
            validation = self._read_validation(shard)
        encode_meta: dict[str, Any] = {}
        encode_directory = self.workspace.shard_dir(shard.shard_id, "encode")
        try:
            encode_meta = json.loads((encode_directory / "encode.meta.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            encode_meta = {}
        return {
            "shard_id": shard.shard_id,
            "stages": {
                stage: {
                    "status": "reused" if any(item["stage"] == stage and item["shard_id"] == shard.shard_id for item in reused) else "rebuilt",
                    "sequence": stage_records.get(stage, {}).get("sequence"),
                    "fingerprint": expected[stage],
                    "artifact_digest": stage_records.get(stage, {}).get("artifact_digest"),
                }
                for stage in stages
            },
            "reused_stages": reused,
            "rebuilt_stages": rebuilt,
            "validation": validation,
            "degradations": encode_meta.get("degradations", []),
            "preferred_format": shard.mask_format,
        }

    def _requirement(self, shard: ShardSpec, stage: str, expected: Mapping[str, str]) -> dict[str, Any]:
        record = self._find_valid_checkpoint(shard.shard_id, stage, expected[stage])
        if record is None:
            raise PipelineError(
                E_INVALID_STAGE,
                "Missing prerequisite stage artifact",
                {"shard_id": shard.shard_id, "stage": stage},
            )
        return record

    def _read_validation(self, shard: ShardSpec) -> dict[str, Any]:
        path = self.workspace.shard_dir(shard.shard_id, "validate") / "validation.json"
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise PipelineError(E_INVALID_STAGE, "Validation artifact is missing or invalid") from exc

    def _all_stages_current(
        self,
        manifest: Manifest,
        config: RuntimeConfig,
        shard: ShardSpec,
    ) -> bool:
        source_fp = self._source_fingerprint(shard)
        expected = self._expected(self._axes(manifest, shard, config, source_fp))
        return all(
            self._find_valid_checkpoint(shard.shard_id, stage, expected[stage]) is not None
            for stage in STAGES
        )

    def _summary_digest(self, shard_results: Mapping[str, Any]) -> str:
        content: dict[str, Any] = {}
        for shard_id in sorted(shard_results):
            result = shard_results[shard_id]
            validation = result.get("validation") or {}
            stages = result.get("stages", {})
            content[shard_id] = {
                "model_digest": validation.get("model_digest"),
                "validation_artifact_digest": stages.get("validate", {}).get("artifact_digest"),
                "encode_artifact_digest": stages.get("encode", {}).get("artifact_digest"),
                "validation_ok": validation.get("ok"),
                "error_codes": validation.get("error_codes", []),
            }
        return digest_json(content)

    def export(self, shard_id: str, format_name: str) -> tuple[bytes, str]:
        manifest, config = self._load()
        if shard_id not in manifest.shard_by_id:
            raise PipelineError(E_NOT_FOUND, "Unknown shard", {"shard_id": shard_id})
        format_name = {"png": "png_palette", "rle_string": "rle", "json": "coco_json"}.get(format_name, format_name)
        if format_name not in FORMAT_FILE_NAMES:
            raise PipelineError(E_INVALID_CONFIG, "Unknown export format", {"format": format_name})
        shard = manifest.shard_by_id[shard_id]
        state = self._status_for_shard(manifest, shard, config)
        encode_stage = state["stages"].get("encode", {})
        if encode_stage.get("status") != "ready":
            raise PipelineError(
                E_INVALID_STAGE,
                "Current encode artifact is unavailable",
                {"shard_id": shard_id, "stage": "encode", "status": encode_stage.get("status")},
            )
        path = self.workspace.shard_dir(shard_id, "encode") / FORMAT_FILE_NAMES[format_name]
        if not path.is_file():
            try:
                degradations = json.loads(
                    (self.workspace.shard_dir(shard_id, "encode") / "degradations.json").read_text(encoding="utf-8")
                )
            except (OSError, json.JSONDecodeError):
                degradations = []
            matching = next(
                (item for item in degradations if isinstance(item, Mapping) and item.get("format") == format_name),
                None,
            )
            code = matching.get("error_code") if matching else "E_FORMAT_MISSING"
            raise PipelineError(
                code,
                "Requested format was explicitly degraded and not produced",
                {"shard_id": shard_id, "format": format_name, "degradation": matching},
            )
        content_type = {
            "png_palette": "image/png",
            "rle": "text/plain; charset=utf-8",
            "coco_json": "application/json",
        }[format_name]
        return path.read_bytes(), content_type

    def validate(self, shard_ids: Iterable[str] | None = None) -> dict[str, Any]:
        return self.build(shard_ids=shard_ids, stages=["validate"])

    def publish_raw_checkpoint(self, record: Mapping[str, Any], expected_sequence: int | None = None) -> dict[str, Any]:
        return self.checkpoints.publish(dict(record), expected_sequence=expected_sequence)
