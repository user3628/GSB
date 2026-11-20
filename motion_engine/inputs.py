"""Load immutable frame inputs and derive per-step input fingerprints."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .errors import invalid_state, timeline_overflow
from .manifest import (
    MODEL_VERSION,
    expected_range_for_step,
    series_stage_fingerprints,
)
from .util import digest_json, frame_fingerprint, sha256_file


@dataclass(frozen=True)
class StepPlan:
    step: int
    truth_frame_index: int
    step_inputs: dict[str, Any]
    step_fingerprint: str


@dataclass
class SeriesData:
    series: dict[str, Any]
    frames: np.ndarray
    frame_hashes: tuple[str, ...]
    frame_file_sha256: str
    frame_content_fingerprint: str
    manifest_config_fingerprint: str
    stage_fingerprints: dict[str, str]
    version_fingerprint: str
    base_frame_fingerprint: str
    step_plans: tuple[StepPlan, ...]

    @property
    def series_id(self) -> str:
        return str(self.series["series_id"])

    @property
    def initial_state(self) -> dict[str, float]:
        return self.series["initial_state"]

    @property
    def render(self) -> dict[str, Any]:
        return self.series["render"]

    @property
    def prediction_steps(self) -> int:
        return int(self.series["prediction_steps"])

    def truth_frame(self, step: int) -> np.ndarray:
        return self.frames[self.step_plans[step - 1].truth_frame_index]


def _load_frames(series: dict[str, Any]) -> tuple[np.ndarray, str, str]:
    series_id = series["series_id"]
    frame_path = Path(series["frame_path"])
    if not frame_path.is_file():
        raise invalid_state(
            "frame sequence file does not exist",
            series_id=series_id,
            position="frame_file",
            path=str(frame_path),
        )
    try:
        loaded = np.load(frame_path, allow_pickle=False)
    except (OSError, ValueError) as exc:
        raise invalid_state(
            f"cannot load frame sequence: {exc}",
            series_id=series_id,
            position="frame_file",
            path=str(frame_path),
        ) from exc
    if not isinstance(loaded, np.ndarray) or loaded.ndim != 3:
        raise invalid_state(
            "frame sequence must have shape [frames, height, width]",
            series_id=series_id,
            position="frames.shape",
            actual_shape=list(loaded.shape) if isinstance(loaded, np.ndarray) else None,
        )
    if loaded.dtype.kind not in {"i", "u"} or loaded.dtype == np.bool_:
        raise invalid_state(
            "frame sequence dtype must be an integer type",
            series_id=series_id,
            position="frames.dtype",
            actual_dtype=str(loaded.dtype),
        )
    minimum = int(loaded.min(initial=0))
    maximum = int(loaded.max(initial=0))
    if minimum < 0 or maximum > 255:
        raise invalid_state(
            "frame pixels must be in the inclusive range 0..255",
            series_id=series_id,
            position="frames.pixels",
            minimum=minimum,
            maximum=maximum,
        )
    actual_count = int(loaded.shape[0])
    if actual_count != int(series["frame_count"]):
        raise invalid_state(
            "manifest frame_count does not match the actual frame sequence",
            series_id=series_id,
            position="frame_count",
            expected=series["frame_count"],
            actual=actual_count,
        )
    if actual_count > int(series["frame_upper_bound"]):
        raise invalid_state(
            "frame sequence exceeds manifest frame_upper_bound",
            series_id=series_id,
            position="frame_upper_bound",
            upper_bound=series["frame_upper_bound"],
            actual=actual_count,
        )
    if actual_count < 1 or loaded.shape[1] < 1 or loaded.shape[2] < 1:
        raise invalid_state(
            "frame sequence dimensions must be positive",
            series_id=series_id,
            position="frames.shape",
            actual_shape=list(loaded.shape),
        )
    frames = np.ascontiguousarray(loaded, dtype=np.uint8)
    return frames, sha256_file(frame_path), digest_json(
        {
            "schema": "motion-frame-sequence-v1",
            "shape": [int(part) for part in frames.shape],
            "frame_sha256": [frame_fingerprint(frame) for frame in frames],
        }
    )


def _timeline_plan(series: dict[str, Any]) -> list[int]:
    steps = int(series["prediction_steps"])
    origin = int(series["timeline"]["origin_frame"])
    stride = int(series["timeline"]["stride_frames"])
    frame_count = int(series["frame_count"])
    last_required = origin + steps * stride
    available_last = frame_count - 1
    if last_required > available_last:
        raise timeline_overflow(
            str(series["series_id"]),
            overflow_frames=last_required - available_last,
            last_required_frame=last_required,
            frame_count=frame_count,
        )
    return [origin + step * stride for step in range(1, steps + 1)]


def load_series_data(series: dict[str, Any]) -> SeriesData:
    frames, frame_file_sha256, frame_content_fingerprint = _load_frames(series)
    truth_indices = _timeline_plan(series)
    frame_hashes = tuple(frame_fingerprint(frame) for frame in frames)
    stage_fingerprints = series_stage_fingerprints(
        series, series["render"], frame_content_fingerprint
    )
    manifest_config_fingerprint = digest_json(
        {
            "schema": "motion-manifest-config-v1",
            "model_version": MODEL_VERSION,
            "series": {
                key: series[key]
                for key in (
                    "series_id",
                    "frame_file",
                    "frame_count",
                    "frame_upper_bound",
                    "prediction_steps",
                    "timeline",
                    "initial_state",
                    "dynamics",
                    "deformation",
                    "expected_state_ranges",
                    "render",
                )
            },
        }
    )
    version_fingerprint = digest_json(
        {
            "schema": "motion-series-version-v1",
            **stage_fingerprints,
            "manifest_config": manifest_config_fingerprint,
        }
    )

    plans: list[StepPlan] = []
    for step, truth_index in enumerate(truth_indices, start=1):
        step_inputs = {
            "base_frame": frame_hashes[0],
            "truth_frame": frame_hashes[truth_index],
            "truth_frame_index": truth_index,
            "initial_state": stage_fingerprints["initial_state"],
            "dynamics": stage_fingerprints["dynamics"],
            "deformation": stage_fingerprints["deformation"],
            "timeline": stage_fingerprints["timeline"],
            "render": stage_fingerprints["render"],
            "expected_ranges": digest_json(
                expected_range_for_step(series, step)
            ),
        }
        step_fingerprint = digest_json(
            {
                "schema": "motion-step-input-v1",
                "model_version": MODEL_VERSION,
                "series_id": series["series_id"],
                "step": step,
                "inputs": step_inputs,
            }
        )
        plans.append(
            StepPlan(
                step=step,
                truth_frame_index=truth_index,
                step_inputs=step_inputs,
                step_fingerprint=step_fingerprint,
            )
        )

    return SeriesData(
        series=series,
        frames=frames,
        frame_hashes=frame_hashes,
        frame_file_sha256=frame_file_sha256,
        frame_content_fingerprint=frame_content_fingerprint,
        manifest_config_fingerprint=manifest_config_fingerprint,
        stage_fingerprints=stage_fingerprints,
        version_fingerprint=version_fingerprint,
        base_frame_fingerprint=frame_hashes[0],
        step_plans=tuple(plans),
    )
