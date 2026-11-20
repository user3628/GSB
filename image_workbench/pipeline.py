from __future__ import annotations

import base64
import copy
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

import numpy as np

from .algorithms import repair_pixels, style_transfer_pixels
from .codecs import descriptor_bytes, encode_jpeg, encode_png
from .errors import E_NOT_FOUND, E_VALIDATION, WorkbenchError
from .fingerprints import STAGES, compute_stage_fingerprints, explain_invalidation
from .hashing import digest_array, digest_json
from .manifest import validate_format_config
from .model import FragmentModel


def _encode_array(array: np.ndarray) -> dict[str, Any]:
    contiguous = np.ascontiguousarray(array)
    return {
        "encoding": "base64-raw-rgb-u8",
        "width": int(contiguous.shape[1]),
        "height": int(contiguous.shape[0]),
        "channels": 3,
        "data": base64.b64encode(contiguous.tobytes()).decode("ascii"),
        "sha256": digest_array(contiguous, role="rgb"),
    }


def _decode_array(value: dict[str, Any]) -> np.ndarray:
    width = int(value["width"])
    height = int(value["height"])
    raw = base64.b64decode(value["data"], validate=True)
    if len(raw) != width * height * 3:
        raise WorkbenchError(E_VALIDATION, "Cached pixel payload length mismatch")
    array = np.frombuffer(raw, dtype=np.uint8).reshape(height, width, 3).copy()
    if value.get("sha256") and digest_array(array, role="rgb") != value["sha256"]:
        raise WorkbenchError(E_VALIDATION, "Cached pixel payload digest mismatch")
    return array


@dataclass
class ExportBundle:
    output_format: str
    descriptor: dict[str, Any]
    image_bytes: bytes | None = None
    text_bytes: bytes | None = None

    @property
    def semantic_digest(self) -> str:
        descriptor_digest = digest_json(self.descriptor)
        return digest_json(
            {
                "output_format": self.output_format,
                "descriptor_digest": descriptor_digest,
                "source_pixel_digest": self.descriptor.get("image", {}).get("source_pixel_digest"),
                "payload_present": self.image_bytes is not None or self.text_bytes is not None,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "output_format": self.output_format,
            "descriptor": copy.deepcopy(self.descriptor),
            "image_base64": base64.b64encode(self.image_bytes).decode("ascii") if self.image_bytes is not None else None,
            "text_base64": base64.b64encode(self.text_bytes).decode("ascii") if self.text_bytes is not None else None,
            "semantic_digest": self.semantic_digest,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> ExportBundle:
        return cls(
            output_format=str(value["output_format"]),
            descriptor=copy.deepcopy(value["descriptor"]),
            image_bytes=base64.b64decode(value["image_base64"]) if value.get("image_base64") else None,
            text_bytes=base64.b64decode(value["text_base64"]) if value.get("text_base64") else None,
        )


@dataclass
class FragmentCache:
    fingerprints: dict[str, str] = field(default_factory=dict)
    dependencies: dict[str, dict[str, Any]] = field(default_factory=dict)
    artifacts: dict[str, Any] = field(default_factory=dict)
    statuses: dict[str, dict[str, Any]] = field(default_factory=dict)
    earliest_invalidated_stage: str | None = None

    def to_dict(self) -> dict[str, Any]:
        artifacts: dict[str, Any] = {}
        if "repair" in self.artifacts:
            artifacts["repair"] = _encode_array(self.artifacts["repair"])
        if "style" in self.artifacts:
            artifacts["style"] = _encode_array(self.artifacts["style"])
        if "export" in self.artifacts:
            artifacts["export"] = self.artifacts["export"].to_dict()
        return {
            "fingerprints": copy.deepcopy(self.fingerprints),
            "dependencies": copy.deepcopy(self.dependencies),
            "artifacts": artifacts,
            "statuses": copy.deepcopy(self.statuses),
            "earliest_invalidated_stage": self.earliest_invalidated_stage,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> FragmentCache:
        artifacts: dict[str, Any] = {}
        raw_artifacts = value.get("artifacts", {})
        if "repair" in raw_artifacts:
            artifacts["repair"] = _decode_array(raw_artifacts["repair"])
        if "style" in raw_artifacts:
            artifacts["style"] = _decode_array(raw_artifacts["style"])
        if "export" in raw_artifacts:
            artifacts["export"] = ExportBundle.from_dict(raw_artifacts["export"])
        return cls(
            fingerprints=copy.deepcopy(value.get("fingerprints", {})),
            dependencies=copy.deepcopy(value.get("dependencies", {})),
            artifacts=artifacts,
            statuses=copy.deepcopy(value.get("statuses", {})),
            earliest_invalidated_stage=value.get("earliest_invalidated_stage"),
        )


@dataclass
class PipelineCache:
    fragments: dict[str, FragmentCache] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"cache_version": 1, "fragments": {key: value.to_dict() for key, value in self.fragments.items()}}

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> PipelineCache:
        if not value:
            return cls()
        return cls(
            fragments={
                key: FragmentCache.from_dict(item) for key, item in value.get("fragments", {}).items()
            }
        )


@dataclass
class FragmentPipelineResult:
    fragment_id: str
    source: FragmentModel
    repaired_pixels: np.ndarray
    styled_pixels: np.ndarray
    export_bundle: ExportBundle
    cache: FragmentCache
    stage_records: list[dict[str, Any]]

    def summary_dict(self) -> dict[str, Any]:
        return {
            "fragment_id": self.fragment_id,
            "source_pixel_digest": self.source.pixel_digest,
            "repair_pixel_digest": digest_array(self.repaired_pixels, role="rgb"),
            "style_pixel_digest": digest_array(self.styled_pixels, role="rgb"),
            "export_format": self.export_bundle.output_format,
            "export_semantic_digest": self.export_bundle.semantic_digest,
            "stage_fingerprints": {stage: self.cache.fingerprints.get(stage) for stage in STAGES},
        }


@dataclass
class PipelineRunResult:
    results: dict[str, FragmentPipelineResult]
    cache: PipelineCache
    processed_order: list[str]
    stopped: bool
    stop_after_fragment: str | None

    @property
    def summary_digest(self) -> str:
        return digest_json(
            {
                "processed_order": self.processed_order,
                "fragments": [self.results[fragment_id].summary_dict() for fragment_id in self.processed_order],
            }
        )

    def to_summary(self) -> dict[str, Any]:
        return {
            "summary_digest": self.summary_digest,
            "processed_order": list(self.processed_order),
            "stopped": self.stopped,
            "stop_after_fragment": self.stop_after_fragment,
            "fragments": {
                fragment_id: self.results[fragment_id].summary_dict()
                for fragment_id in self.processed_order
            },
        }


class StageRecordSink(Protocol):
    def __call__(self, record: dict[str, Any]) -> None: ...


def _export_model(model: FragmentModel, styled_pixels: np.ndarray, format_config: dict[str, Any]) -> ExportBundle:
    exported_model = model.with_pixels(styled_pixels)
    if exported_model.output_format == "png":
        png_config = format_config.get("png", {})
        image_bytes, descriptor = encode_png(
            exported_model,
            compress_level=int(png_config.get("compress_level", 6)),
        )
        return ExportBundle("png", descriptor, image_bytes=image_bytes)
    if exported_model.output_format == "jpeg":
        jpeg_config = format_config.get("jpeg", {})
        image_bytes, descriptor = encode_jpeg(
            exported_model,
            quality=int(jpeg_config.get("quality", 95)),
            subsampling=int(jpeg_config.get("subsampling", 0)),
        )
        return ExportBundle("jpeg", descriptor, image_bytes=image_bytes)
    from .codecs import descriptor_for_model

    descriptor = descriptor_for_model(exported_model, "png", loss_items=exported_model.loss_items)
    text = descriptor_bytes(descriptor)
    return ExportBundle("json", descriptor, text_bytes=text)


def run_pipeline(
    models: list[FragmentModel],
    format_config: dict[str, Any],
    *,
    previous_cache: PipelineCache | None = None,
    stop_requested: Callable[[], bool] | None = None,
    record_sink: StageRecordSink | None = None,
    on_fragment_complete: Callable[[str], None] | None = None,
) -> PipelineRunResult:
    if not models:
        raise WorkbenchError(E_VALIDATION, "Pipeline requires at least one fragment")
    format_config = validate_format_config(format_config)
    by_id = {model.fragment_id: model for model in models}
    if len(by_id) != len(models):
        raise WorkbenchError(E_VALIDATION, "Pipeline input contains duplicate fragment identifiers")
    cache_in = previous_cache or PipelineCache()
    fingerprints_by_id: dict[str, dict[str, Any]] = {}
    for model in models:
        reference = by_id.get(model.style_reference)
        if reference is None:
            raise WorkbenchError(
                E_NOT_FOUND,
                "Style reference fragment not found",
                fragment_id=model.fragment_id,
                details={"style_reference": model.style_reference},
            )
        fingerprints_by_id[model.fragment_id] = compute_stage_fingerprints(model, reference, format_config)
    results: dict[str, FragmentPipelineResult] = {}
    processed: list[str] = []
    stopped = False
    stop_after: str | None = None
    for model in models:
        if stop_requested is not None and stop_requested():
            stopped = True
            break
        fragment_id = model.fragment_id
        previous = cache_in.fragments.get(fragment_id)
        current_fps = fingerprints_by_id[fragment_id]
        earliest: str | None = None
        if previous is None or not previous.fingerprints:
            earliest = "source"
            details_reason = "initial build"
        else:
            for stage in STAGES:
                if previous.fingerprints.get(stage) != current_fps[stage].fingerprint:
                    earliest = stage
                    details_reason = explain_invalidation(
                        stage,
                        previous.dependencies.get(stage, {}),
                        current_fps[stage].dependencies,
                    )
                    break
        if earliest is None:
            details_reason = "all stage fingerprints unchanged"
        earliest_index = STAGES.index(earliest) if earliest is not None else len(STAGES)
        stage_statuses: dict[str, dict[str, Any]] = {}
        stage_records: list[dict[str, Any]] = []
        for index, stage in enumerate(STAGES):
            if previous is None or not previous.fingerprints:
                reused = False
                invalidated = True
                reason = "initial build"
            elif index < earliest_index:
                reused = True
                invalidated = False
                reason = "fingerprint unchanged; upstream artifact reused"
            elif index == earliest_index:
                reused = False
                invalidated = True
                reason = details_reason
            else:
                reused = False
                invalidated = True
                reason = f"downstream of invalidated {earliest} stage"
            stage_statuses[stage] = {
                "fingerprint": current_fps[stage].fingerprint,
                "input_fingerprint": current_fps[stage].input_fingerprint,
                "config_fingerprint": current_fps[stage].config_fingerprint,
                "dependencies": copy.deepcopy(current_fps[stage].dependencies),
                "reused": reused,
                "invalidated": invalidated,
                "rebuilt": not reused,
                "reason": reason,
            }

        def emit(
            stage: str,
            artifact_digest: str,
            *,
            _fragment_id: str = fragment_id,
            _current_fps: dict[str, Any] = current_fps,
            _stage_statuses: dict[str, dict[str, Any]] = stage_statuses,
            _stage_records: list[dict[str, Any]] = stage_records,
        ) -> None:
            record = {
                "fragment_id": _fragment_id,
                "stage": stage,
                "input_fingerprint": _current_fps[stage].input_fingerprint,
                "config_fingerprint": _current_fps[stage].config_fingerprint,
                "artifact_digest": artifact_digest,
                "status": "reused" if _stage_statuses[stage]["reused"] else "rebuilt",
                "reason": _stage_statuses[stage]["reason"],
            }
            _stage_records.append(record)
            if record_sink is not None:
                record_sink(record)

        emit("source", model.pixel_digest)
        if previous is not None and "repair" in previous.artifacts and stage_statuses["repair"]["reused"]:
            repaired = np.array(previous.artifacts["repair"], dtype=np.uint8, copy=True)
        else:
            repaired_result = repair_pixels(
                model.pixels,
                model.mask_pixels,
                model.repair_iterations,
                fragment_id=fragment_id,
            )
            repaired = repaired_result.pixels
        emit("repair", digest_array(repaired, role="rgb"))
        if previous is not None and "style" in previous.artifacts and stage_statuses["style"]["reused"]:
            styled = np.array(previous.artifacts["style"], dtype=np.uint8, copy=True)
        else:
            reference = by_id[model.style_reference]
            styled_result = style_transfer_pixels(
                repaired,
                reference.pixels,
                model.style_strength,
                fragment_id=fragment_id,
            )
            styled = styled_result.pixels
        emit("style", digest_array(styled, role="rgb"))
        if previous is not None and "export" in previous.artifacts and stage_statuses["export"]["reused"]:
            export_bundle = previous.artifacts["export"]
        else:
            export_bundle = _export_model(model, styled, format_config)
        emit("export", export_bundle.semantic_digest)
        entry = FragmentCache(
            fingerprints={stage: current_fps[stage].fingerprint for stage in STAGES},
            dependencies={stage: copy.deepcopy(current_fps[stage].dependencies) for stage in STAGES},
            artifacts={"repair": repaired, "style": styled, "export": export_bundle},
            statuses=stage_statuses,
            earliest_invalidated_stage=earliest,
        )
        result = FragmentPipelineResult(
            fragment_id=fragment_id,
            source=model,
            repaired_pixels=repaired,
            styled_pixels=styled,
            export_bundle=export_bundle,
            cache=entry,
            stage_records=stage_records,
        )
        results[fragment_id] = result
        processed.append(fragment_id)
        if on_fragment_complete is not None:
            on_fragment_complete(fragment_id)
        if stop_requested is not None and stop_requested():
            stop_after = fragment_id
            stopped = True
            break
    new_cache = PipelineCache(fragments=dict(cache_in.fragments))
    for fragment_id, result in results.items():
        new_cache.fragments[fragment_id] = result.cache
    return PipelineRunResult(
        results=results,
        cache=new_cache,
        processed_order=processed,
        stopped=stopped,
        stop_after_fragment=stop_after,
    )

