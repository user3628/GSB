"""Numeric, timeline, manifest, and project validation."""

from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .config import Config
from .errors import ToneLabError
from .models import Project, Shard


_FLOAT_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")


def parse_finite_float(value: Any, *, field: str, shard_id: str, position: Any) -> float:
    try:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.lower() in {"nan", "+nan", "-nan", "inf", "+inf", "-inf", "infinity"}:
                parsed = float(stripped)
            elif _FLOAT_RE.match(stripped):
                parsed = float(stripped)
            else:
                raise ValueError("not a decimal number")
        else:
            parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ToneLabError(
            "E_NUMERIC",
            f"{field} is not a finite decimal number",
            shard_id=shard_id,
            position=position,
            context={"field": field, "value_repr": repr(value), "reason": str(exc)},
        ) from exc
    if not math.isfinite(parsed):
        raise ToneLabError(
            "E_NUMERIC",
            f"{field} must be finite",
            shard_id=shard_id,
            position=position,
            context={"field": field, "value_repr": repr(value)},
        )
    return parsed


def validate_shard(shard: Shard, config: Config, *, normalized: bool = True) -> None:
    if shard.sample_rate <= 0 or shard.channels <= 0 or shard.bit_depth <= 0:
        raise ToneLabError(
            "E_FORMAT_MISMATCH",
            "shard declares an invalid audio format",
            shard_id=shard.shard_id,
            context={
                "declared": {
                    "sample_rate": shard.sample_rate,
                    "channels": shard.channels,
                    "bit_depth": shard.bit_depth,
                },
                "actual": None,
            },
        )
    if shard.pcm.ndim != 1 or shard.pcm.dtype != np.int16:
        raise ToneLabError(
            "E_FORMAT_MISMATCH",
            "internal PCM must be a mono int16 sequence",
            shard_id=shard.shard_id,
            context={"actual": {"shape": list(shard.pcm.shape), "dtype": str(shard.pcm.dtype)}},
        )
    if shard.structure_version < 1:
        raise ToneLabError(
            "E_VALIDATION",
            "structure_version must be >= 1",
            shard_id=shard.shard_id,
            context={"structure_version": shard.structure_version},
        )
    validate_pitch_curve(shard, config)
    validate_units(shard, config)
    if not math.isfinite(float(shard.reference_f0_mean)) or shard.reference_f0_mean <= 0:
        raise ToneLabError(
            "E_NUMERIC",
            "reference_f0_mean must be a positive finite number",
            shard_id=shard.shard_id,
            position={"kind": "reference_f0_mean"},
            context={"value_repr": repr(shard.reference_f0_mean)},
        )


def validate_pitch_curve(shard: Shard, config: Config) -> None:
    if not shard.pitch_curve:
        raise ToneLabError(
            "E_NUMERIC",
            "pitch curve must contain at least one point",
            shard_id=shard.shard_id,
            position={"kind": "pitch_curve", "index": 0},
            context={"point_count": 0},
        )
    pitch_min = float(config.get("model.pitch_min"))
    pitch_max = float(config.get("model.pitch_max"))
    previous_time = -math.inf
    for index, point in enumerate(shard.pitch_curve):
        if not isinstance(point, (tuple, list)) or len(point) != 2:
            raise ToneLabError(
                "E_NUMERIC",
                "pitch point must be a (time, value) pair",
                shard_id=shard.shard_id,
                position={"kind": "pitch_point", "index": index},
                context={"value_repr": repr(point)},
            )
        time_value = parse_finite_float(
            point[0],
            field="pitch_curve.time",
            shard_id=shard.shard_id,
            position={"kind": "pitch_point", "index": index, "component": "time"},
        )
        pitch_value = parse_finite_float(
            point[1],
            field="pitch_curve.value",
            shard_id=shard.shard_id,
            position={"kind": "pitch_point", "index": index, "component": "value"},
        )
        if time_value < 0.0 or time_value > 1.0 or time_value < previous_time:
            raise ToneLabError(
                "E_TIMELINE_INVALID",
                "pitch curve times must be normalized, monotonic, and within [0, 1]",
                shard_id=shard.shard_id,
                position={"kind": "pitch_point", "index": index, "time": time_value},
                context={
                    "previous_time": previous_time,
                    "allowed_time": [0.0, 1.0],
                },
            )
        if pitch_value < pitch_min or pitch_value > pitch_max:
            raise ToneLabError(
                "E_NUMERIC",
                "normalized pitch value is outside the configured range",
                shard_id=shard.shard_id,
                position={"kind": "pitch_point", "index": index, "time": time_value},
                context={
                    "value": pitch_value,
                    "allowed_range": [pitch_min, pitch_max],
                    "field": "pitch_curve.value",
                },
            )
        previous_time = time_value


def validate_units(shard: Shard, config: Config) -> None:
    duration_ms = shard.duration_ms
    previous_start = -math.inf
    for index, unit in enumerate(shard.units):
        start = parse_finite_float(
            unit.start_ms,
            field="unit.start_ms",
            shard_id=shard.shard_id,
            position={"kind": "unit", "unit_index": index, "component": "start_ms"},
        )
        end = parse_finite_float(
            unit.end_ms,
            field="unit.end_ms",
            shard_id=shard.shard_id,
            position={"kind": "unit", "unit_index": index, "component": "end_ms"},
        )
        if start < 0.0 or end < 0.0:
            raise ToneLabError(
                "E_TIMELINE_INVALID",
                "unit interval must not be negative",
                shard_id=shard.shard_id,
                unit_index=index,
                position={"kind": "unit", "unit_index": index, "field": "start/end"},
                context={"start_ms": start, "end_ms": end, "duration_ms": duration_ms},
            )
        if end < start:
            raise ToneLabError(
                "E_TIMELINE_INVALID",
                "unit end_ms must not be earlier than start_ms",
                shard_id=shard.shard_id,
                unit_index=index,
                position={"kind": "unit", "unit_index": index, "field": "end_ms"},
                context={"start_ms": start, "end_ms": end},
            )
        if end > duration_ms + 1e-9:
            raise ToneLabError(
                "E_TIMELINE_INVALID",
                "unit interval exceeds shard duration",
                shard_id=shard.shard_id,
                unit_index=index,
                position={"kind": "unit", "unit_index": index, "field": "end_ms"},
                context={
                    "start_ms": start,
                    "end_ms": end,
                    "duration_ms": duration_ms,
                    "overflow_ms": end - duration_ms,
                },
            )
        if start < previous_start:
            raise ToneLabError(
                "E_TIMELINE_INVALID",
                "units must be ordered by start_ms",
                shard_id=shard.shard_id,
                unit_index=index,
                position={"kind": "unit", "unit_index": index, "field": "start_ms"},
                context={"start_ms": start, "previous_start_ms": previous_start},
            )
        previous_start = start
        if not unit.symbol:
            raise ToneLabError(
                "E_VALIDATION",
                "unit symbol must not be empty",
                shard_id=shard.shard_id,
                unit_index=index,
                position={"kind": "unit", "unit_index": index, "field": "symbol"},
            )
        if len(unit.formants_hz) != int(config.get("model.formant_count")):
            raise ToneLabError(
                "E_NUMERIC",
                "unit must declare the configured number of formants",
                shard_id=shard.shard_id,
                unit_index=index,
                position={"kind": "unit", "unit_index": index, "field": "formants_hz"},
                context={
                    "expected": int(config.get("model.formant_count")),
                    "actual": len(unit.formants_hz),
                },
            )
        for formant_index, formant in enumerate(unit.formants_hz):
            parsed = parse_finite_float(
                formant,
                field="unit.formants_hz",
                shard_id=shard.shard_id,
                position={
                    "kind": "unit",
                    "unit_index": index,
                    "component": "formants_hz",
                    "formant_index": formant_index,
                },
            )
            if parsed <= 0.0 or parsed > shard.sample_rate / 2.0:
                raise ToneLabError(
                    "E_NUMERIC",
                    "formant frequency must be positive and below Nyquist",
                    shard_id=shard.shard_id,
                    unit_index=index,
                    position={
                        "kind": "unit",
                        "unit_index": index,
                        "formant_index": formant_index,
                    },
                    context={
                        "value": parsed,
                        "allowed_range": [0.0, shard.sample_rate / 2.0],
                    },
                )
        if unit.formant_ranges_hz:
            if len(unit.formant_ranges_hz) != len(unit.formants_hz):
                raise ToneLabError(
                    "E_NUMERIC",
                    "formant range count must match formant target count",
                    shard_id=shard.shard_id,
                    unit_index=index,
                    position={"kind": "unit", "unit_index": index, "field": "formant_ranges_hz"},
                    context={
                        "target_count": len(unit.formants_hz),
                        "range_count": len(unit.formant_ranges_hz),
                    },
                )
            for formant_index, pair in enumerate(unit.formant_ranges_hz):
                if not isinstance(pair, (list, tuple)) or len(pair) != 2:
                    raise ToneLabError(
                        "E_NUMERIC",
                        "formant range must be a [low, high] pair",
                        shard_id=shard.shard_id,
                        unit_index=index,
                        position={
                            "kind": "unit",
                            "unit_index": index,
                            "formant_index": formant_index,
                            "field": "formant_ranges_hz",
                        },
                        context={"value_repr": repr(pair)},
                    )
                low = parse_finite_float(
                    pair[0],
                    field="unit.formant_ranges_hz.low",
                    shard_id=shard.shard_id,
                    position={"kind": "unit", "unit_index": index, "formant_index": formant_index},
                )
                high = parse_finite_float(
                    pair[1],
                    field="unit.formant_ranges_hz.high",
                    shard_id=shard.shard_id,
                    position={"kind": "unit", "unit_index": index, "formant_index": formant_index},
                )
                if low < 0.0 or high < low or high > shard.sample_rate / 2.0:
                    raise ToneLabError(
                        "E_NUMERIC",
                        "formant range must be ordered, non-negative, and below Nyquist",
                        shard_id=shard.shard_id,
                        unit_index=index,
                        position={
                            "kind": "unit",
                            "unit_index": index,
                            "formant_index": formant_index,
                            "field": "formant_ranges_hz",
                        },
                        context={"low": low, "high": high},
                    )


def validate_project(project: Project, config: Config) -> None:
    seen: set[str] = set()
    for shard in project.shards:
        if shard.shard_id in seen:
            raise ToneLabError(
                "E_VALIDATION",
                "duplicate shard id",
                shard_id=shard.shard_id,
                context={"shard_id": shard.shard_id},
            )
        seen.add(shard.shard_id)
        validate_shard(shard, config)
    total = project.total_samples
    declared = int(project.declared_total_samples)
    if total > declared:
        raise ToneLabError(
            "E_MANIFEST_MISMATCH",
            "sum of actual shard samples exceeds declared total sample count",
            context={"declared_total_samples": declared, "actual_total_samples": total},
        )
    if bool(config.get("manifest.exact_total_samples")) and total != declared:
        raise ToneLabError(
            "E_MANIFEST_MISMATCH",
            "declared total sample count does not equal actual shard sample total",
            context={"declared_total_samples": declared, "actual_total_samples": total},
        )
    for shard in project.shards:
        if shard.sample_rate != project.sample_rate:
            raise ToneLabError(
                "E_FORMAT_MISMATCH",
                "shard sample rate differs from project sample rate",
                shard_id=shard.shard_id,
                context={
                    "declared": {"sample_rate": project.sample_rate},
                    "actual": {"sample_rate": shard.sample_rate},
                },
            )
        if shard.channels != project.channels:
            raise ToneLabError(
                "E_FORMAT_MISMATCH",
                "shard channel count differs from project channel count",
                shard_id=shard.shard_id,
                context={
                    "declared": {"channels": project.channels},
                    "actual": {"channels": shard.channels},
                },
            )
        if shard.bit_depth != project.bit_depth:
            raise ToneLabError(
                "E_FORMAT_MISMATCH",
                "shard bit depth differs from project bit depth",
                shard_id=shard.shard_id,
                context={
                    "declared": {"bit_depth": project.bit_depth},
                    "actual": {"bit_depth": shard.bit_depth},
                },
            )


def check_manifest_totals(
    declared_total: int,
    shard_counts: Iterable[tuple[str, int | None, int]],
    config: Config,
) -> None:
    actual_total = 0
    for shard_id, declared_count, actual_count in shard_counts:
        actual_total += actual_count
        if declared_count is None and bool(config.get("manifest.require_shard_sample_count")):
            raise ToneLabError(
                "E_MANIFEST_MISMATCH",
                "manifest shard is missing sample_count",
                shard_id=shard_id,
                context={"declared_sample_count": None, "actual_sample_count": actual_count},
            )
        if declared_count is not None and actual_count > int(declared_count):
            raise ToneLabError(
                "E_MANIFEST_MISMATCH",
                "actual shard sample count exceeds the declared shard upper bound",
                shard_id=shard_id,
                context={
                    "declared_sample_count": int(declared_count),
                    "actual_sample_count": actual_count,
                },
            )
        if (
            declared_count is not None
            and bool(config.get("manifest.exact_total_samples"))
            and actual_count != int(declared_count)
        ):
            raise ToneLabError(
                "E_MANIFEST_MISMATCH",
                "actual shard sample count does not equal declared sample_count",
                shard_id=shard_id,
                context={
                    "declared_sample_count": int(declared_count),
                    "actual_sample_count": actual_count,
                },
            )
    if actual_total > int(declared_total):
        raise ToneLabError(
            "E_MANIFEST_MISMATCH",
            "sum of actual shard samples exceeds declared manifest total",
            context={"declared_total_samples": int(declared_total), "actual_total_samples": actual_total},
        )
    if bool(config.get("manifest.exact_total_samples")) and actual_total != int(declared_total):
        raise ToneLabError(
            "E_MANIFEST_MISMATCH",
            "declared manifest total sample count does not equal actual shard total",
            context={"declared_total_samples": int(declared_total), "actual_total_samples": actual_total},
        )

