"""Bidirectional codecs for projects, manifests, WAV sidecars, and unit labels."""

from __future__ import annotations

import copy
import math
from pathlib import Path
from typing import Any, Iterable

import numpy as np

from .audio import inspect_wav, read_wav, write_wav
from .config import Config
from .errors import ToneLabError
from .models import Project, PronUnit, Shard
from .util import (
    atomic_write_bytes,
    atomic_write_json,
    canonical_json_bytes,
    decode_pcm_int16,
    encode_pcm_int16,
    pretty_json_bytes,
    read_json,
    safe_component,
    sha256_bytes,
    sha256_file,
)
from .validation import check_manifest_totals, parse_finite_float, validate_shard


def _manifest_audio(data: dict[str, Any], config: Config) -> dict[str, int]:
    raw = data.get("audio", {})
    return {
        "sample_rate": int(raw.get("sample_rate", config.audio["sample_rate"])),
        "channels": int(raw.get("channels", config.audio["channels"])),
        "bit_depth": int(raw.get("bit_depth", config.audio["bit_depth"])),
    }


def parse_pitch_curve(raw: Any, *, shard_id: str) -> list[tuple[float, float]]:
    if raw is None or raw == [] or raw == {}:
        return [(0.0, 1.0)]
    if isinstance(raw, dict):
        raw = raw.get("points", raw.get("curve", []))
        if not raw:
            return [(0.0, 1.0)]
    if not isinstance(raw, list):
        raise ToneLabError(
            "E_NUMERIC",
            "pitch_curve must be a list of points",
            shard_id=shard_id,
            position={"kind": "pitch_curve", "index": 0},
            context={"value_repr": repr(raw)[:300]},
        )
    result: list[tuple[float, float]] = []
    for index, point in enumerate(raw):
        position = {"kind": "pitch_point", "index": index}
        if isinstance(point, dict):
            if "time" in point:
                time_raw = point["time"]
            elif "time_ratio" in point:
                time_raw = point["time_ratio"]
            elif "t" in point:
                time_raw = point["t"]
            else:
                raise ToneLabError(
                    "E_NUMERIC",
                    "pitch point object is missing time",
                    shard_id=shard_id,
                    position=position,
                    context={"keys": sorted(point.keys())},
                )
            if "value" in point:
                value_raw = point["value"]
            elif "pitch" in point:
                value_raw = point["pitch"]
            elif "normalized_pitch" in point:
                value_raw = point["normalized_pitch"]
            else:
                raise ToneLabError(
                    "E_NUMERIC",
                    "pitch point object is missing value",
                    shard_id=shard_id,
                    position=position,
                    context={"keys": sorted(point.keys())},
                )
        elif isinstance(point, (list, tuple)) and len(point) == 2:
            time_raw, value_raw = point
        else:
            raise ToneLabError(
                "E_NUMERIC",
                "pitch point must be [time, value] or an object",
                shard_id=shard_id,
                position=position,
                context={"value_repr": repr(point)},
            )
        time_value = parse_finite_float(
            time_raw,
            field="pitch_curve.time",
            shard_id=shard_id,
            position={**position, "component": "time"},
        )
        pitch_value = parse_finite_float(
            value_raw,
            field="pitch_curve.value",
            shard_id=shard_id,
            position={**position, "component": "value"},
        )
        result.append((time_value, pitch_value))
    return result


def _parse_formant_value(raw: Any, *, shard_id: str, unit_index: int, formant_index: int) -> tuple[float, list[float] | None]:
    position = {
        "kind": "unit",
        "unit_index": unit_index,
        "formant_index": formant_index,
        "field": "formants_hz",
    }
    if isinstance(raw, (list, tuple)):
        if len(raw) != 2:
            raise ToneLabError(
                "E_NUMERIC",
                "formant target interval must be [low, high]",
                shard_id=shard_id,
                unit_index=unit_index,
                position=position,
                context={"value_repr": repr(raw)},
            )
        low = parse_finite_float(raw[0], field="formant_range.low", shard_id=shard_id, position=position)
        high = parse_finite_float(raw[1], field="formant_range.high", shard_id=shard_id, position=position)
        if low < 0.0 or high < low:
            raise ToneLabError(
                "E_NUMERIC",
                "formant interval must be non-negative and ordered",
                shard_id=shard_id,
                unit_index=unit_index,
                position=position,
                context={"low": low, "high": high},
            )
        return (low + high) / 2.0, [low, high]
    value = parse_finite_float(raw, field="formants_hz", shard_id=shard_id, position=position)
    return value, None


def _default_formants(symbol: str, config: Config) -> tuple[list[float], list[list[float]]]:
    compat = config.data["v1_compat"]
    pause_symbols = {str(item).lower() for item in compat.get("pause_symbols", [])}
    front_vowels = {str(item).lower() for item in compat.get("front_vowel_symbols", [])}
    lower = symbol.lower()
    if lower in pause_symbols:
        key = "pause"
    elif lower in front_vowels or any(ch in symbol for ch in "衣一语音iI"):
        key = "front_vowel"
    elif any(ch in lower for ch in "aeiou") or any(ch in symbol for ch in "啊哦额爱欧"):
        key = "vowel"
    else:
        key = "fallback"
    defaults = compat["default_formants"]
    targets = [float(x) for x in defaults.get(key, defaults["fallback"])]
    ranges: list[list[float]] = []
    for value in targets:
        width = max(40.0, value * 0.08)
        ranges.append([round(value - width, 6), round(value + width, 6)])
    return targets, ranges


def _unit_id(shard_id: str, index: int, raw: Any = None) -> str:
    if raw:
        return str(raw)
    return f"{shard_id}:u{index:04d}"


def _parse_v2_units(
    raw_units: Any,
    *,
    shard_id: str,
    formant_targets: dict[str, Any] | None,
    config: Config,
) -> list[PronUnit]:
    if not isinstance(raw_units, list):
        raise ToneLabError(
            "E_VALIDATION",
            "version 2 units must be a list",
            shard_id=shard_id,
            context={"actual_type": type(raw_units).__name__},
        )
    units: list[PronUnit] = []
    for index, item in enumerate(raw_units):
        if not isinstance(item, dict):
            raise ToneLabError(
                "E_VALIDATION",
                "each version 2 unit must be an object",
                shard_id=shard_id,
                unit_index=index,
                position={"kind": "unit", "unit_index": index},
            )
        symbol = str(item.get("symbol", ""))
        start_ms = parse_finite_float(
            item.get("start_ms"),
            field="unit.start_ms",
            shard_id=shard_id,
            position={"kind": "unit", "unit_index": index, "component": "start_ms"},
        )
        end_ms = parse_finite_float(
            item.get("end_ms"),
            field="unit.end_ms",
            shard_id=shard_id,
            position={"kind": "unit", "unit_index": index, "component": "end_ms"},
        )
        raw_formants = item.get("formants_hz")
        if raw_formants is None and formant_targets:
            raw_formants = formant_targets.get(symbol)
        if raw_formants is None:
            raise ToneLabError(
                "E_VALIDATION",
                "version 2 unit is missing formant targets",
                shard_id=shard_id,
                unit_index=index,
                position={"kind": "unit", "unit_index": index, "field": "formants_hz"},
            )
        if not isinstance(raw_formants, list):
            raise ToneLabError(
                "E_NUMERIC",
                "formants_hz must be a list",
                shard_id=shard_id,
                unit_index=index,
                position={"kind": "unit", "unit_index": index, "field": "formants_hz"},
            )
        targets: list[float] = []
        ranges: list[list[float]] = []
        explicit_ranges = item.get("formant_ranges_hz")
        for formant_index, raw_formant in enumerate(raw_formants):
            value, inferred = _parse_formant_value(
                raw_formant,
                shard_id=shard_id,
                unit_index=index,
                formant_index=formant_index,
            )
            targets.append(value)
            if isinstance(explicit_ranges, list) and formant_index < len(explicit_ranges):
                pair = explicit_ranges[formant_index]
                low = parse_finite_float(
                    pair[0] if isinstance(pair, (list, tuple)) and len(pair) == 2 else pair,
                    field="formant_range.low",
                    shard_id=shard_id,
                    position={"kind": "unit", "unit_index": index, "formant_index": formant_index},
                )
                high = parse_finite_float(
                    pair[1] if isinstance(pair, (list, tuple)) and len(pair) == 2 else pair,
                    field="formant_range.high",
                    shard_id=shard_id,
                    position={"kind": "unit", "unit_index": index, "formant_index": formant_index},
                )
                ranges.append([low, high])
            elif inferred is not None:
                ranges.append(inferred)
            else:
                width = max(40.0, value * 0.08)
                ranges.append([value - width, value + width])
        units.append(
            PronUnit(
                symbol=symbol,
                start_ms=start_ms,
                end_ms=end_ms,
                formants_hz=targets,
                formant_ranges_hz=ranges,
                unit_id=_unit_id(shard_id, index, item.get("unit_id", item.get("id"))),
            )
        )
    return units


def _read_shard_pcm(
    item: dict[str, Any],
    *,
    manifest_path: Path,
    shard_id: str,
    audio_format: dict[str, int],
) -> tuple[np.ndarray, dict[str, Any]]:
    if "pcm_b64" in item:
        try:
            pcm = decode_pcm_int16(str(item["pcm_b64"]))
        except Exception as exc:
            raise ToneLabError(
                "E_FORMAT_MISMATCH",
                "embedded PCM is not valid int16 little-endian base64",
                shard_id=shard_id,
                context={"reason": str(exc)},
            ) from exc
        encoding = str(item.get("pcm_encoding", "int16le-base64"))
        if encoding != "int16le-base64":
            raise ToneLabError(
                "E_FORMAT_MISMATCH",
                "unsupported embedded PCM encoding",
                shard_id=shard_id,
                context={"pcm_encoding": encoding},
            )
        return pcm, {"source": "embedded", "pcm_encoding": encoding}
    wav_value = item.get("wav") or item.get("audio_path") or item.get("path")
    if not wav_value:
        raise ToneLabError(
            "E_FORMAT_MISMATCH",
            "manifest shard must declare wav or embedded pcm_b64",
            shard_id=shard_id,
            context={"keys": sorted(item.keys())},
        )
    wav_path = Path(str(wav_value))
    if not wav_path.is_absolute():
        wav_path = manifest_path.parent / wav_path
    pcm, info = read_wav(
        wav_path,
        shard_id=shard_id,
        expected_sample_rate=int(item.get("sample_rate", audio_format["sample_rate"])),
        expected_channels=int(item.get("channels", audio_format["channels"])),
        expected_bit_depth=int(item.get("bit_depth", audio_format["bit_depth"])),
    )
    return pcm, {"source": "wav", "wav": str(wav_value), "wav_info": info}


def load_manifest(path: str | Path, config: Config) -> Project:
    manifest_path = Path(path)
    data = read_json(manifest_path)
    if not isinstance(data, dict):
        raise ToneLabError("E_VALIDATION", "manifest root must be a JSON object")
    version_raw = data.get("structure_version", data.get("version"))
    if version_raw is None:
        raise ToneLabError(
            "E_VALIDATION",
            "manifest is missing structure_version",
            context={"manifest": str(manifest_path)},
        )
    try:
        version = int(version_raw)
    except (TypeError, ValueError) as exc:
        raise ToneLabError(
            "E_VALIDATION",
            "structure_version must be an integer",
            context={"value": repr(version_raw)},
        ) from exc
    if version == 2:
        return _load_v2(manifest_path, data, config)
    if version == 1:
        return _load_v1(manifest_path, data, config)
    raise ToneLabError(
        "E_VALIDATION",
        "unsupported structure_version",
        context={"structure_version": version, "supported": [1, 2]},
    )


def _load_v2(manifest_path: Path, data: dict[str, Any], config: Config) -> Project:
    audio_format = _manifest_audio(data, config)
    if "declared_total_samples" not in data:
        raise ToneLabError(
            "E_MANIFEST_MISMATCH",
            "manifest is missing declared_total_samples",
            context={"manifest": str(manifest_path)},
        )
    declared_total = int(data["declared_total_samples"])
    raw_shards = data.get("shards")
    if not isinstance(raw_shards, list) or not raw_shards:
        raise ToneLabError("E_VALIDATION", "manifest shards must be a non-empty list")
    shards: list[Shard] = []
    count_rows: list[tuple[str, int | None, int]] = []
    for index, item in enumerate(raw_shards):
        if not isinstance(item, dict):
            raise ToneLabError("E_VALIDATION", "manifest shard must be an object", position={"shard_index": index})
        shard_id = str(item.get("shard_id", item.get("id", f"shard-{index:04d}")))
        pcm, source_meta = _read_shard_pcm(
            item,
            manifest_path=manifest_path,
            shard_id=shard_id,
            audio_format=audio_format,
        )
        declared_count = item.get("sample_count")
        count_rows.append((shard_id, int(declared_count) if declared_count is not None else None, len(pcm)))
        units = _parse_v2_units(
            item.get("units", []),
            shard_id=shard_id,
            formant_targets=item.get("formant_targets"),
            config=config,
        )
        shard = Shard(
            shard_id=shard_id,
            timbre_id=str(item.get("timbre_id", "unknown")),
            pcm=pcm,
            pitch_curve=parse_pitch_curve(item.get("pitch_curve", []), shard_id=shard_id),
            units=units,
            structure_version=2,
            source_structure_version=2,
            reference_f0_mean=parse_finite_float(
                item.get("reference_f0_mean", 180.0),
                field="reference_f0_mean",
                shard_id=shard_id,
                position={"kind": "shard", "field": "reference_f0_mean"},
            ),
            sample_rate=int(item.get("sample_rate", audio_format["sample_rate"])),
            channels=int(item.get("channels", audio_format["channels"])),
            bit_depth=int(item.get("bit_depth", audio_format["bit_depth"])),
            origin={
                "manifest": str(manifest_path.name),
                "manifest_index": index,
                **source_meta,
            },
        )
        validate_shard(shard, config)
        shards.append(shard)
    check_manifest_totals(declared_total, count_rows, config)
    project = Project(
        project_id=str(data.get("project_id", manifest_path.stem)),
        shards=shards,
        description=str(data.get("description", "")),
        sample_rate=audio_format["sample_rate"],
        channels=audio_format["channels"],
        bit_depth=audio_format["bit_depth"],
        declared_total_samples=declared_total,
        source_manifests=[str(manifest_path)],
        metadata={"manifest_format": str(data.get("format", "tonelab.manifest.v2"))},
    )
    return project


def _stable_v1_id(item: dict[str, Any], index: int, manifest_path: Path, config: Config) -> str:
    existing = item.get("shard_id", item.get("id"))
    if existing:
        return str(existing)
    compat = config.data["v1_compat"]
    if item.get("legacy_key"):
        stable_key = str(item["legacy_key"])
    else:
        raw_key = item.get("wav") or item.get("path") or item.get("timbre_id") or "unknown"
        stable_key = Path(str(raw_key)).name
    payload = {
        "stable_key": str(stable_key),
        "timbre_id": str(item.get("timbre_id", "unknown")),
        "index": index,
        "sample_count": item.get("sample_count"),
        "pitch_offsets_percent": item.get("pitch_offsets_percent", item.get("pitch_offset_percent")),
        "units": item.get("units"),
        "structure_version": 1,
    }
    digest = sha256_bytes(canonical_json_bytes(payload))[: int(compat.get("stable_id_length", 12))]
    return f"{compat.get('stable_id_prefix', 'v1-')}{digest}"


def _percent_to_normalized(percent: int, reference_f0: float, config: Config) -> float:
    compat = config.data["v1_compat"]
    scale = float(compat.get("pitch_percent_scale", 0.01))
    mode = str(compat.get("pitch_percent_mode", "reference_ratio"))
    if mode == "reference_ratio":
        return 1.0 + float(percent) * scale
    if mode == "cents":
        # A percentage point contributes `scale * 100` cents.
        return float(2.0 ** ((float(percent) * scale) / 12.0))
    if mode == "absolute_offset_hz":
        return (reference_f0 + float(percent) * scale * reference_f0) / reference_f0
    raise ToneLabError(
        "E_VALIDATION",
        "unsupported v1 pitch percentage conversion mode",
        context={"pitch_percent_mode": mode, "supported": ["reference_ratio", "cents", "absolute_offset_hz"]},
    )


def _normalized_to_percent(value: float, reference_f0: float, config: Config) -> int:
    compat = config.data["v1_compat"]
    scale = float(compat.get("pitch_percent_scale", 0.01))
    mode = str(compat.get("pitch_percent_mode", "reference_ratio"))
    if mode == "reference_ratio":
        percent = (float(value) - 1.0) / scale
    elif mode == "cents":
        percent = 12.0 * math.log2(float(value)) / scale
    elif mode == "absolute_offset_hz":
        percent = (float(value) - 1.0) / scale
    else:
        raise ToneLabError(
            "E_VALIDATION",
            "unsupported v1 pitch percentage conversion mode",
            context={"pitch_percent_mode": mode},
        )
    return int(math.floor(percent + 0.5))


def _legacy_percentages(raw: Any, shard_id: str) -> list[int]:
    if raw is None:
        return [0]
    if not isinstance(raw, list):
        raw = [raw]
    result: list[int] = []
    for index, value in enumerate(raw):
        position = {"kind": "pitch_offset_percent", "index": index}
        parsed = parse_finite_float(
            value,
            field="pitch_offsets_percent",
            shard_id=shard_id,
            position=position,
        )
        rounded = int(round(parsed))
        if abs(parsed - rounded) > 1e-9:
            raise ToneLabError(
                "E_NUMERIC",
                "version 1 pitch offsets must be integer percentages",
                shard_id=shard_id,
                position=position,
                context={"value": parsed, "value_repr": repr(value)},
            )
        result.append(rounded)
    return result


def _load_v1(manifest_path: Path, data: dict[str, Any], config: Config) -> Project:
    compat = data.get("v1_compat", {}) if isinstance(data.get("v1_compat", {}), dict) else {}
    # Manifest-level v1_compat can be used by tests and fixtures, but configured
    # defaults remain authoritative unless a caller deliberately supplies them.
    separator = str(compat.get("unit_separator", config.get("v1_compat.unit_separator", "|")))
    if separator == "":
        raise ToneLabError("E_VALIDATION", "v1 unit separator must not be empty")
    audio_format = _manifest_audio(data, config)
    if "declared_total_samples" not in data:
        raise ToneLabError(
            "E_MANIFEST_MISMATCH",
            "version 1 manifest is missing declared_total_samples",
            context={"manifest": str(manifest_path)},
        )
    declared_total = int(data["declared_total_samples"])
    raw_shards = data.get("shards")
    if not isinstance(raw_shards, list) or not raw_shards:
        raise ToneLabError("E_VALIDATION", "version 1 manifest shards must be a non-empty list")
    shards: list[Shard] = []
    count_rows: list[tuple[str, int | None, int]] = []
    seen_ids: set[str] = set()
    for index, item in enumerate(raw_shards):
        if not isinstance(item, dict):
            raise ToneLabError("E_VALIDATION", "version 1 shard must be an object", position={"shard_index": index})
        shard_id = _stable_v1_id(item, index, manifest_path, config)
        if shard_id in seen_ids:
            raise ToneLabError(
                "E_VALIDATION",
                "generated version 1 stable shard id collided",
                shard_id=shard_id,
                context={"index": index},
            )
        seen_ids.add(shard_id)
        pcm, source_meta = _read_shard_pcm(
            item,
            manifest_path=manifest_path,
            shard_id=shard_id,
            audio_format=audio_format,
        )
        declared_count = item.get("sample_count")
        count_rows.append((shard_id, int(declared_count) if declared_count is not None else None, len(pcm)))
        duration_ms = len(pcm) * 1000.0 / float(item.get("sample_rate", audio_format["sample_rate"]))
        raw_units = item.get("units", "")
        if isinstance(raw_units, list):
            raise ToneLabError(
                "E_VALIDATION",
                "version 1 units must be one whole string",
                shard_id=shard_id,
                context={"actual_type": "list", "value_repr": repr(raw_units)[:200]},
            )
        symbols = str(raw_units).split(separator)
        if not symbols or any(symbol == "" for symbol in symbols):
            raise ToneLabError(
                "E_VALIDATION",
                "version 1 unit string contains an empty symbol",
                shard_id=shard_id,
                context={"unit_string_repr": repr(raw_units), "separator": separator},
            )
        unit_count = len(symbols)
        units: list[PronUnit] = []
        for unit_index, symbol in enumerate(symbols):
            start_ms = duration_ms * unit_index / unit_count
            end_ms = duration_ms * (unit_index + 1) / unit_count
            formants, ranges = _default_formants(symbol, config)
            units.append(
                PronUnit(
                    symbol=symbol,
                    start_ms=round(start_ms, 6),
                    end_ms=round(end_ms, 6),
                    formants_hz=formants,
                    formant_ranges_hz=ranges,
                    unit_id=f"{shard_id}:u{unit_index:04d}",
                )
            )
        percentages = _legacy_percentages(
            item.get("pitch_offsets_percent", item.get("pitch_offset_percent")),
            shard_id,
        )
        if len(percentages) == 1:
            curve = [(0.0, _percent_to_normalized(percentages[0], float(item.get("reference_f0_mean", 180.0)), config))]
        else:
            curve = [
                (
                    round(point_index / (len(percentages) - 1), 12),
                    _percent_to_normalized(percent, float(item.get("reference_f0_mean", 180.0)), config),
                )
                for point_index, percent in enumerate(percentages)
            ]
        shard = Shard(
            shard_id=shard_id,
            timbre_id=str(item.get("timbre_id", "unknown")),
            pcm=pcm,
            pitch_curve=curve,
            units=units,
            structure_version=2,
            source_structure_version=1,
            reference_f0_mean=parse_finite_float(
                item.get("reference_f0_mean", 180.0),
                field="reference_f0_mean",
                shard_id=shard_id,
                position={"kind": "shard", "field": "reference_f0_mean"},
            ),
            sample_rate=int(item.get("sample_rate", audio_format["sample_rate"])),
            channels=int(item.get("channels", audio_format["channels"])),
            bit_depth=int(item.get("bit_depth", audio_format["bit_depth"])),
            origin={
                "manifest": str(manifest_path.name),
                "manifest_index": index,
                "upgraded_from": 1,
                "v1_unit_string": str(raw_units),
                "v1_unit_separator": separator,
                "v1_pitch_offsets_percent": percentages,
                **source_meta,
            },
        )
        validate_shard(shard, config)
        shards.append(shard)
    check_manifest_totals(declared_total, count_rows, config)
    return Project(
        project_id=str(data.get("project_id", manifest_path.stem)),
        shards=shards,
        description=str(data.get("description", "")),
        sample_rate=audio_format["sample_rate"],
        channels=audio_format["channels"],
        bit_depth=audio_format["bit_depth"],
        declared_total_samples=declared_total,
        source_manifests=[str(manifest_path)],
        metadata={"manifest_format": str(data.get("format", "tonelab.manifest.v1")), "upgraded_from": 1},
    )


def import_manifests(paths: Iterable[str | Path], config: Config, *, project_id: str = "demo-project") -> Project:
    shards: list[Shard] = []
    manifests: list[str] = []
    descriptions: list[str] = []
    for path in paths:
        project = load_manifest(path, config)
        manifests.extend(project.source_manifests)
        if project.description:
            descriptions.append(project.description)
        shards.extend(project.shards)
    seen: dict[str, Shard] = {}
    for shard in shards:
        if shard.shard_id in seen:
            raise ToneLabError(
                "E_VALIDATION",
                "duplicate shard id while merging manifests",
                shard_id=shard.shard_id,
                context={"first_manifest": seen[shard.shard_id].origin.get("manifest"), "second_manifest": shard.origin.get("manifest")},
            )
        seen[shard.shard_id] = shard
    project = Project(
        project_id=project_id,
        shards=shards,
        description=" / ".join(descriptions),
        declared_total_samples=sum(shard.sample_count for shard in shards),
        source_manifests=manifests,
        metadata={"imported_manifests": len(manifests)},
    )
    return project


def shard_to_v2_dict(shard: Shard, *, embed_pcm: bool = True) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "shard_id": shard.shard_id,
        "timbre_id": shard.timbre_id,
        "sample_count": shard.sample_count,
        "sample_rate": shard.sample_rate,
        "channels": shard.channels,
        "bit_depth": shard.bit_depth,
        "reference_f0_mean": shard.reference_f0_mean,
        "structure_version": 2,
        "source_structure_version": shard.source_structure_version,
        "pitch_curve": [[t, v] for t, v in shard.pitch_curve],
        "units": [unit.to_dict() for unit in shard.units],
        "quantization": shard.quantization,
    }
    if embed_pcm:
        payload["pcm_encoding"] = "int16le-base64"
        payload["pcm_b64"] = encode_pcm_int16(shard.pcm)
    return payload


def project_to_v2_dict(project: Project, *, embed_pcm: bool = True) -> dict[str, Any]:
    return {
        "format": "tonelab.sound-project",
        "structure_version": 2,
        "project_id": project.project_id,
        "description": project.description,
        "audio": {
            "sample_rate": project.sample_rate,
            "channels": project.channels,
            "bit_depth": project.bit_depth,
        },
        "declared_total_samples": project.declared_total_samples,
        "shards": [shard_to_v2_dict(shard, embed_pcm=embed_pcm) for shard in project.shards],
    }


def export_project_json_v2(project: Project, path: str | Path, *, embed_pcm: bool = True) -> dict[str, Any]:
    payload = project_to_v2_dict(project, embed_pcm=embed_pcm)
    data = pretty_json_bytes(payload)
    atomic_write_bytes(path, data)
    return {
        "format": "json_v2",
        "path": str(path),
        "sha256": sha256_bytes(data),
        "bytes": len(data),
        "shards": len(project.shards),
    }


def project_to_v1_dict(
    project: Project,
    *,
    wav_names: dict[str, str],
    config: Config,
) -> dict[str, Any]:
    separator = str(config.get("v1_compat.unit_separator", "|"))
    shards_payload: list[dict[str, Any]] = []
    for shard in project.shards:
        percentages = [
            _normalized_to_percent(value, shard.reference_f0_mean, config)
            for _time, value in shard.pitch_curve
        ]
        shards_payload.append(
            {
                "shard_id": shard.shard_id,
                "wav": wav_names[shard.shard_id],
                "timbre_id": shard.timbre_id,
                "reference_f0_mean": shard.reference_f0_mean,
                "sample_count": shard.sample_count,
                "pitch_offsets_percent": percentages,
                "units": separator.join(unit.symbol for unit in shard.units),
            }
        )
    return {
        "format": "tonelab.manifest.v1",
        "structure_version": 1,
        "project_id": project.project_id,
        "description": project.description,
        "audio": {
            "sample_rate": project.sample_rate,
            "channels": project.channels,
            "bit_depth": project.bit_depth,
        },
        "declared_total_samples": project.declared_total_samples,
        "shards": shards_payload,
        "upgrade_note": "Export intentionally uses the version 1 string/percentage schema.",
    }


def export_wav_bundle(project: Project, out_dir: str | Path) -> dict[str, Any]:
    root = Path(out_dir)
    root.mkdir(parents=True, exist_ok=True)
    files: dict[str, Any] = {}
    for shard in project.shards:
        stem = safe_component(shard.shard_id)
        wav_path = root / f"{stem}.wav"
        sidecar_path = root / f"{stem}.pitch.json"
        info = write_wav(
            wav_path,
            shard.pcm,
            sample_rate=shard.sample_rate,
            channels=shard.channels,
            bit_depth=shard.bit_depth,
            shard_id=shard.shard_id,
        )
        sidecar = {
            "format": "tonelab.wav-sidecar",
            "sidecar_version": 1,
            "structure_version": shard.structure_version,
            "source_structure_version": shard.source_structure_version,
            "shard": {
                **shard.to_internal_dict(embed_pcm=False),
                "wav": wav_path.name,
            },
        }
        sidecar["shard"].pop("origin", None)
        sidecar_bytes = pretty_json_bytes(sidecar)
        atomic_write_bytes(sidecar_path, sidecar_bytes)
        files[shard.shard_id] = {
            "wav": str(wav_path),
            "wav_sha256": sha256_file(wav_path),
            "sidecar": str(sidecar_path),
            "sidecar_sha256": sha256_bytes(sidecar_bytes),
            "sample_rate": info["sample_rate"],
            "channels": info["channels"],
            "bit_depth": info["bit_depth"],
        }
    return {"format": "wav", "path": str(root), "files": files}


def export_project_json_v1(project: Project, out_dir: str | Path, config: Config) -> dict[str, Any]:
    root = Path(out_dir)
    wav_dir = root / "wav"
    root.mkdir(parents=True, exist_ok=True)
    wav_names: dict[str, str] = {}
    bundle = export_wav_bundle(project, wav_dir)
    for shard in project.shards:
        wav_names[shard.shard_id] = str(Path(bundle["files"][shard.shard_id]["wav"]).relative_to(root))
    payload = project_to_v1_dict(project, wav_names=wav_names, config=config)
    manifest_path = root / "manifest_v1.json"
    data = pretty_json_bytes(payload)
    atomic_write_bytes(manifest_path, data)
    return {
        "format": "json_v1",
        "path": str(manifest_path),
        "sha256": sha256_bytes(data),
        "wav_bundle": bundle,
        "pitch_percent_rounding_error_bound": 0.5 * float(config.get("v1_compat.pitch_percent_scale", 0.01)),
    }


def export_units_annotations(project: Project, out_dir: str | Path) -> dict[str, Any]:
    root = Path(out_dir)
    root.mkdir(parents=True, exist_ok=True)
    files: dict[str, str] = {}
    for shard in project.shards:
        payload = {
            "format": "tonelab.units-annotation",
            "structure_version": 2,
            "shard_id": shard.shard_id,
            "sample_rate": shard.sample_rate,
            "sample_count": shard.sample_count,
            "units": [
                {
                    "unit_id": unit.unit_id,
                    "symbol": unit.symbol,
                    "start_ms": unit.start_ms,
                    "end_ms": unit.end_ms,
                }
                for unit in shard.units
            ],
        }
        path = root / f"{safe_component(shard.shard_id)}.units.json"
        atomic_write_bytes(path, pretty_json_bytes(payload))
        files[shard.shard_id] = str(path)
    return {"format": "units", "path": str(root), "files": files}


def load_wav_sidecar(wav_path: str | Path, sidecar_path: str | Path, config: Config) -> Shard:
    payload = read_json(sidecar_path)
    if payload.get("format") != "tonelab.wav-sidecar":
        raise ToneLabError("E_VALIDATION", "not a tonelab WAV sidecar", context={"path": str(sidecar_path)})
    shard_data = dict(payload.get("shard", {}))
    shard_id = str(shard_data.get("shard_id", Path(wav_path).stem))
    expected_rate = int(shard_data.get("sample_rate", config.audio["sample_rate"]))
    expected_channels = int(shard_data.get("channels", config.audio["channels"]))
    expected_bits = int(shard_data.get("bit_depth", config.audio["bit_depth"]))
    pcm, _info = read_wav(
        wav_path,
        shard_id=shard_id,
        expected_sample_rate=expected_rate,
        expected_channels=expected_channels,
        expected_bit_depth=expected_bits,
    )
    declared_count = int(shard_data.get("sample_count", len(pcm)))
    if len(pcm) > declared_count or (bool(config.get("manifest.exact_total_samples")) and len(pcm) != declared_count):
        raise ToneLabError(
            "E_MANIFEST_MISMATCH",
            "WAV sample count does not match sidecar declaration",
            shard_id=shard_id,
            context={"declared_sample_count": declared_count, "actual_sample_count": len(pcm)},
        )
    units = [
        PronUnit(
            symbol=str(item.get("symbol", "")),
            start_ms=float(item.get("start_ms", 0.0)),
            end_ms=float(item.get("end_ms", 0.0)),
            formants_hz=[float(x) for x in item.get("formants_hz", [])],
            formant_ranges_hz=[
                [float(pair[0]), float(pair[1])]
                for pair in item.get("formant_ranges_hz", [])
            ],
            unit_id=str(item.get("unit_id", "")),
        )
        for item in shard_data.get("units", [])
    ]
    shard = Shard(
        shard_id=shard_id,
        timbre_id=str(shard_data.get("timbre_id", "unknown")),
        pcm=pcm,
        pitch_curve=parse_pitch_curve(shard_data.get("pitch_curve", []), shard_id=shard_id),
        units=units,
        structure_version=int(shard_data.get("structure_version", 2)),
        source_structure_version=int(shard_data.get("source_structure_version", 2)),
        reference_f0_mean=float(shard_data.get("reference_f0_mean", 180.0)),
        sample_rate=expected_rate,
        channels=expected_channels,
        bit_depth=expected_bits,
        origin={"source": "wav", "wav": str(wav_path), "sidecar": str(sidecar_path)},
        quantization=dict(shard_data.get("quantization", {})),
    )
    validate_shard(shard, config)
    return shard


def import_wav_bundle(path_or_dir: str | Path, config: Config) -> Project:
    root = Path(path_or_dir)
    if root.is_file():
        if root.suffix.lower() != ".wav":
            raise ToneLabError("E_VALIDATION", "WAV import expects a .wav file or directory")
        sidecar = root.with_suffix(".pitch.json")
        if not sidecar.exists():
            sidecar = root.with_name(root.stem + ".pitch.json")
        shards = [load_wav_sidecar(root, sidecar, config)]
        project_id = root.stem
    else:
        sidecars = sorted(root.glob("*.pitch.json"))
        if not sidecars:
            raise ToneLabError("E_VALIDATION", "WAV directory contains no *.pitch.json sidecars")
        shards = []
        for sidecar in sidecars:
            payload = read_json(sidecar)
            wav_name = payload.get("shard", {}).get("wav", sidecar.name.replace(".pitch.json", ".wav"))
            wav_path = root / str(wav_name)
            shards.append(load_wav_sidecar(wav_path, sidecar, config))
        project_id = root.name
    return Project(
        project_id=project_id,
        shards=shards,
        declared_total_samples=sum(shard.sample_count for shard in shards),
        metadata={"format": "wav"},
    )


def import_project_json(path: str | Path, config: Config) -> Project:
    json_path = Path(path)
    data = read_json(json_path)
    version = int(data.get("structure_version", data.get("version", 2)))
    if version == 1:
        return _load_v1(json_path, data, config)
    if version != 2:
        raise ToneLabError("E_VALIDATION", "unsupported project JSON structure_version", context={"structure_version": version})
    audio_format = _manifest_audio(data, config)
    declared_total = int(data.get("declared_total_samples", 0))
    shards: list[Shard] = []
    count_rows: list[tuple[str, int | None, int]] = []
    for index, item in enumerate(data.get("shards", [])):
        if not isinstance(item, dict):
            raise ToneLabError("E_VALIDATION", "project shard must be an object")
        shard_id = str(item.get("shard_id", item.get("id", f"shard-{index:04d}")))
        pcm, source_meta = _read_shard_pcm(item, manifest_path=json_path, shard_id=shard_id, audio_format=audio_format)
        declared_count = item.get("sample_count")
        count_rows.append((shard_id, int(declared_count) if declared_count is not None else None, len(pcm)))
        shard = Shard(
            shard_id=shard_id,
            timbre_id=str(item.get("timbre_id", "unknown")),
            pcm=pcm,
            pitch_curve=parse_pitch_curve(item.get("pitch_curve", []), shard_id=shard_id),
            units=_parse_v2_units(
                item.get("units", []),
                shard_id=shard_id,
                formant_targets=item.get("formant_targets"),
                config=config,
            ),
            structure_version=2,
            source_structure_version=int(item.get("source_structure_version", 2)),
            reference_f0_mean=float(item.get("reference_f0_mean", 180.0)),
            sample_rate=int(item.get("sample_rate", audio_format["sample_rate"])),
            channels=int(item.get("channels", audio_format["channels"])),
            bit_depth=int(item.get("bit_depth", audio_format["bit_depth"])),
            origin={"source": "project_json", **source_meta},
            quantization=dict(item.get("quantization", {})),
        )
        validate_shard(shard, config)
        shards.append(shard)
    if not count_rows:
        raise ToneLabError("E_VALIDATION", "project JSON has no shards")
    if not declared_total:
        declared_total = sum(row[2] for row in count_rows)
    check_manifest_totals(declared_total, count_rows, config)
    return Project(
        project_id=str(data.get("project_id", json_path.stem)),
        shards=shards,
        description=str(data.get("description", "")),
        sample_rate=audio_format["sample_rate"],
        channels=audio_format["channels"],
        bit_depth=audio_format["bit_depth"],
        declared_total_samples=declared_total,
        metadata={"format": "project_json"},
    )


def apply_units_annotation(project: Project, annotation_path: str | Path, config: Config) -> None:
    path = Path(annotation_path)
    if path.is_dir():
        files = sorted(path.glob("*.units.json"))
    else:
        files = [path]
    if not files:
        raise ToneLabError("E_VALIDATION", "no units annotations found", context={"path": str(path)})
    for file_path in files:
        payload = read_json(file_path)
        if payload.get("format") != "tonelab.units-annotation":
            raise ToneLabError("E_VALIDATION", "invalid units annotation format", context={"path": str(file_path)})
        shard_id = str(payload.get("shard_id"))
        try:
            shard = project.shard(shard_id)
        except KeyError as exc:
            raise ToneLabError("E_NOT_FOUND", "annotation references unknown shard", shard_id=shard_id) from exc
        if "sample_count" in payload and int(payload["sample_count"]) != shard.sample_count:
            raise ToneLabError(
                "E_MANIFEST_MISMATCH",
                "annotation sample_count does not match shard",
                shard_id=shard_id,
                context={"declared_sample_count": int(payload["sample_count"]), "actual_sample_count": shard.sample_count},
            )
        if "sample_rate" in payload and int(payload["sample_rate"]) != shard.sample_rate:
            raise ToneLabError(
                "E_FORMAT_MISMATCH",
                "annotation sample_rate does not match shard",
                shard_id=shard_id,
                context={
                    "declared": {"sample_rate": shard.sample_rate},
                    "actual": {"sample_rate": int(payload["sample_rate"])},
                },
            )
        existing_by_symbol = {unit.symbol: unit for unit in shard.units}
        existing_by_id = {unit.unit_id: unit for unit in shard.units}
        new_units: list[PronUnit] = []
        for index, item in enumerate(payload.get("units", [])):
            symbol = str(item.get("symbol", ""))
            requested_id = str(item.get("unit_id", ""))
            matched = existing_by_id.get(requested_id)
            if matched is None:
                matched = existing_by_symbol.get(symbol)
            if matched is not None:
                formants = list(matched.formants_hz)
                ranges = copy.deepcopy(matched.formant_ranges_hz)
            else:
                formants, ranges = _default_formants(symbol, config)
            new_units.append(
                PronUnit(
                    symbol=symbol,
                    start_ms=float(item.get("start_ms")),
                    end_ms=float(item.get("end_ms")),
                    formants_hz=formants,
                    formant_ranges_hz=ranges,
                    unit_id=str(item.get("unit_id", f"{shard_id}:u{index:04d}")),
                )
            )
        shard.units = new_units
        validate_shard(shard, config)

