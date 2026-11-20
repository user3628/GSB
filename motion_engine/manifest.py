"""Manifest validation and immutable per-series input fingerprints."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

from .errors import invalid_state
from .util import digest_json, sha256_file

MODEL_VERSION = "motion-warp-closed-loop-v1"
STATE_KEYS = ("x", "y", "vx", "vy", "deformation")


def _finite_number(value: Any, name: str, *, series_id: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise invalid_state(
            f"{name} must be a finite number", series_id=series_id, position=name
        )
    number = float(value)
    if not math.isfinite(number):
        raise invalid_state(
            f"{name} must be finite", series_id=series_id, position=name
        )
    return number


def _integer(value: Any, name: str, *, series_id: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise invalid_state(
            f"{name} must be an integer >= {minimum}",
            series_id=series_id,
            position=name,
        )
    return value


def _validate_render(raw: Any, series_id: str) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise invalid_state(
            "render configuration must be an object",
            series_id=series_id,
            position="render",
        )
    render = {
        "translation_gain": _finite_number(
            raw.get("translation_gain", 1.0),
            "render.translation_gain",
            series_id=series_id,
        ),
        "deformation_scale": _finite_number(
            raw.get("deformation_scale", 1.0),
            "render.deformation_scale",
            series_id=series_id,
        ),
        "edge_mode": raw.get("edge_mode", "clamp"),
    }
    if render["edge_mode"] != "clamp":
        raise invalid_state(
            "render.edge_mode currently supports only 'clamp'",
            series_id=series_id,
            position="render.edge_mode",
        )
    if render["deformation_scale"] < 0:
        raise invalid_state(
            "render.deformation_scale must be non-negative",
            series_id=series_id,
            position="render.deformation_scale",
        )
    return render


def _validate_ranges(raw: Any, series_id: str, prediction_steps: int) -> list[dict[str, list[float]]]:
    if not isinstance(raw, list) or len(raw) != prediction_steps:
        raise invalid_state(
            "expected_state_ranges must contain one range object per prediction step",
            series_id=series_id,
            position="expected_state_ranges",
        )
    result: list[dict[str, list[float]]] = []
    for step, item in enumerate(raw, start=1):
        if not isinstance(item, dict):
            raise invalid_state(
                "each expected state range must be an object",
                series_id=series_id,
                position=f"expected_state_ranges[{step - 1}]",
            )
        normalized: dict[str, list[float]] = {}
        for key in STATE_KEYS:
            pair = item.get(key)
            if not isinstance(pair, list) or len(pair) != 2:
                raise invalid_state(
                    f"range for {key} must be [minimum, maximum]",
                    series_id=series_id,
                    position=f"expected_state_ranges[{step - 1}].{key}",
                )
            low = _finite_number(pair[0], f"range.{key}.min", series_id=series_id)
            high = _finite_number(pair[1], f"range.{key}.max", series_id=series_id)
            if low > high:
                raise invalid_state(
                    f"range minimum for {key} exceeds maximum",
                    series_id=series_id,
                    position=f"expected_state_ranges[{step - 1}].{key}",
                )
            normalized[key] = [low, high]
        result.append(normalized)
    return result


def validate_manifest(data: Any, manifest_path: Path) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise invalid_state("manifest root must be a JSON object", position="root")
    if data.get("schema_version") != 1:
        raise invalid_state(
            "unsupported manifest schema_version", position="schema_version"
        )
    if not isinstance(data.get("seed"), int) or isinstance(data.get("seed"), bool):
        raise invalid_state("manifest seed must be an integer", position="seed")
    defaults = data.get("render_defaults", {})
    if not isinstance(defaults, dict):
        raise invalid_state("render_defaults must be an object", position="render_defaults")
    raw_series = data.get("series")
    if not isinstance(raw_series, list) or not raw_series:
        raise invalid_state("manifest must contain a non-empty series list", position="series")

    seen: set[str] = set()
    normalized_series: list[dict[str, Any]] = []
    for index, raw in enumerate(raw_series):
        position = f"series[{index}]"
        if not isinstance(raw, dict):
            raise invalid_state("series entry must be an object", position=position)
        series_id = raw.get("series_id")
        if not isinstance(series_id, str) or not series_id:
            raise invalid_state("series_id must be a non-empty string", position=position)
        if series_id in seen:
            raise invalid_state(
                "duplicate series_id", series_id=series_id, position=position
            )
        seen.add(series_id)

        frame_file = raw.get("frame_file")
        if not isinstance(frame_file, str) or not frame_file:
            raise invalid_state(
                "frame_file must be a non-empty path",
                series_id=series_id,
                position="frame_file",
            )
        resolved_frame_file = (manifest_path.parent / frame_file).resolve()

        prediction_steps = _integer(
            raw.get("prediction_steps"),
            "prediction_steps",
            series_id=series_id,
            minimum=1,
        )
        frame_count = _integer(
            raw.get("frame_count"), "frame_count", series_id=series_id, minimum=2
        )
        frame_upper_bound = _integer(
            raw.get("frame_upper_bound"),
            "frame_upper_bound",
            series_id=series_id,
            minimum=frame_count,
        )
        timeline = raw.get("timeline")
        if not isinstance(timeline, dict):
            raise invalid_state(
                "timeline must be an object", series_id=series_id, position="timeline"
            )
        origin_frame = _integer(
            timeline.get("origin_frame", 0),
            "timeline.origin_frame",
            series_id=series_id,
            minimum=0,
        )
        stride_frames = _integer(
            timeline.get("stride_frames"),
            "timeline.stride_frames",
            series_id=series_id,
            minimum=1,
        )

        initial_raw = raw.get("initial_state")
        if not isinstance(initial_raw, dict):
            raise invalid_state(
                "initial_state must be an object",
                series_id=series_id,
                position="initial_state",
            )
        initial_state = {
            key: _finite_number(
                initial_raw.get(key), f"initial_state.{key}", series_id=series_id
            )
            for key in STATE_KEYS
        }

        dynamics_raw = raw.get("dynamics")
        if not isinstance(dynamics_raw, dict):
            raise invalid_state(
                "dynamics must be an object", series_id=series_id, position="dynamics"
            )
        decay = _finite_number(
            dynamics_raw.get("acceleration_decay"),
            "dynamics.acceleration_decay",
            series_id=series_id,
        )
        if not 0.0 < decay <= 1.0:
            raise invalid_state(
                "dynamics.acceleration_decay must be in (0, 1]",
                series_id=series_id,
                position="dynamics.acceleration_decay",
            )
        event_raw = dynamics_raw.get("direction_event")
        direction_event: dict[str, Any] | None = None
        if event_raw is not None:
            if not isinstance(event_raw, dict):
                raise invalid_state(
                    "direction_event must be an object or null",
                    series_id=series_id,
                    position="dynamics.direction_event",
                )
            event_step = _integer(
                event_raw.get("step"),
                "dynamics.direction_event.step",
                series_id=series_id,
                minimum=1,
            )
            if event_step > prediction_steps:
                raise invalid_state(
                    "direction_event.step must be within prediction_steps",
                    series_id=series_id,
                    position="dynamics.direction_event.step",
                )
            direction_event = {
                "step": event_step,
                "vx_factor": _finite_number(
                    event_raw.get("vx_factor"),
                    "dynamics.direction_event.vx_factor",
                    series_id=series_id,
                ),
                "vy_factor": _finite_number(
                    event_raw.get("vy_factor"),
                    "dynamics.direction_event.vy_factor",
                    series_id=series_id,
                ),
            }

        deformation_raw = raw.get("deformation")
        if not isinstance(deformation_raw, dict):
            raise invalid_state(
                "deformation must be an object",
                series_id=series_id,
                position="deformation",
            )
        amplitude = _finite_number(
            deformation_raw.get("amplitude"),
            "deformation.amplitude",
            series_id=series_id,
        )
        if amplitude < 0:
            raise invalid_state(
                "deformation.amplitude must be non-negative",
                series_id=series_id,
                position="deformation.amplitude",
            )
        deformation = {
            "amplitude": amplitude,
            "frequency": _finite_number(
                deformation_raw.get("frequency"),
                "deformation.frequency",
                series_id=series_id,
            ),
            "phase": _finite_number(
                deformation_raw.get("phase"),
                "deformation.phase",
                series_id=series_id,
            ),
        }

        expected_ranges = _validate_ranges(
            raw.get("expected_state_ranges"), series_id, prediction_steps
        )
        raw_render = raw.get("render", {})
        if not isinstance(raw_render, dict):
            raise invalid_state(
                "render must be an object", series_id=series_id, position="render"
            )
        merged_render_raw = dict(defaults)
        merged_render_raw.update(raw_render)
        render = _validate_render(merged_render_raw, series_id)

        normalized_series.append(
            {
                "series_id": series_id,
                "frame_file": frame_file,
                "frame_path": str(resolved_frame_file),
                "frame_count": frame_count,
                "frame_upper_bound": frame_upper_bound,
                "prediction_steps": prediction_steps,
                "timeline": {
                    "origin_frame": origin_frame,
                    "stride_frames": stride_frames,
                },
                "initial_state": initial_state,
                "dynamics": {
                    "acceleration_decay": decay,
                    "direction_event": direction_event,
                },
                "deformation": deformation,
                "expected_state_ranges": expected_ranges,
                "render": render,
            }
        )

    return {
        "schema_version": 1,
        "seed": data["seed"],
        "model_version": MODEL_VERSION,
        "render_defaults": defaults,
        "series": normalized_series,
        "manifest_path": str(manifest_path),
        "manifest_file_sha256": sha256_file(manifest_path),
    }


def load_manifest(path: str | Path) -> dict[str, Any]:
    from .util import load_json_file

    manifest_path = Path(path).expanduser().resolve()
    if not manifest_path.is_file():
        raise invalid_state(
            "manifest file does not exist", position="manifest_path", path=str(manifest_path)
        )
    try:
        data = load_json_file(manifest_path)
    except (OSError, ValueError) as exc:
        raise invalid_state(
            f"cannot read manifest JSON: {exc}",
            position="manifest_path",
            path=str(manifest_path),
        ) from exc
    return validate_manifest(data, manifest_path)


def series_stage_fingerprints(
    series: dict[str, Any], render: dict[str, Any], frame_content_fingerprint: str
) -> dict[str, str]:
    return {
        "model_version": MODEL_VERSION,
        "frame_content": frame_content_fingerprint,
        "initial_state": digest_json(series["initial_state"]),
        "dynamics": digest_json(series["dynamics"]),
        "deformation": digest_json(series["deformation"]),
        "timeline": digest_json(series["timeline"]),
        "render": digest_json(render),
        "expected_state_ranges": digest_json(series["expected_state_ranges"]),
        "prediction_steps": digest_json(series["prediction_steps"]),
    }


def expected_range_for_step(series: dict[str, Any], step: int) -> dict[str, list[float]]:
    return series["expected_state_ranges"][step - 1]
