"""Configuration parsing, V1 compatibility upgrade and fingerprinting."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .canonical import canonical_json_text, digest_obj, sha256_file
from .errors import TelemetryError


INTERPOLATION_METHODS = {"ffill", "linear", "nearest"}
FUSION_MODES = {"weighted_mean", "priority_override", "priority_group_weighted"}
TIE_BREAK_RULES = {"source_id_lexicographic", "source_order"}


def _as_mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise TelemetryError(
            "E_CONFIG_INVALID",
            f"配置字段 {field} 必须是映射 / {field} must be a mapping",
            {"field": field, "value_type": type(value).__name__},
        )
    return value


def _as_int(value: Any, field: str, *, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TelemetryError(
            "E_CONFIG_INVALID",
            f"配置字段 {field} 必须是整数 / {field} must be an integer",
            {"field": field, "value": value},
        )
    if minimum is not None and value < minimum:
        raise TelemetryError(
            "E_CONFIG_INVALID",
            f"配置字段 {field} 不能小于 {minimum} / invalid range",
            {"field": field, "value": value, "minimum": minimum},
        )
    return value


def _as_float(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TelemetryError(
            "E_CONFIG_INVALID",
            f"配置字段 {field} 必须是数值 / {field} must be numeric",
            {"field": field, "value": value},
        )
    return float(value)


def _round_half_up(numerator: int, denominator: int) -> int:
    return (numerator + denominator // 2) // denominator


def _convert_percent_tolerance(
    percent: Any, basis_ms: Any, rounding: str
) -> tuple[int, dict[str, Any]]:
    percent_int = _as_int(percent, "tolerance_percent", minimum=0)
    basis_int = _as_int(basis_ms, "compatibility.tolerance_basis_ms", minimum=1)
    numerator = percent_int * basis_int
    if rounding == "half_up":
        result = _round_half_up(numerator, 100)
    elif rounding == "floor":
        result = numerator // 100
    elif rounding == "ceil":
        result = (numerator + 99) // 100
    elif rounding == "round":
        result = round(numerator / 100)
    else:
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "不支持的容差取整规则 / unsupported tolerance rounding rule",
            {"rounding": rounding, "allowed": ["half_up", "floor", "ceil", "round"]},
        )
    basis = {
        "source": "v1_tolerance_percent",
        "percent": percent_int,
        "basis_ms": basis_int,
        "rounding": rounding,
        "formula": f"{rounding}({percent_int}% * {basis_int}ms / 100)",
        "tolerance_ms": result,
    }
    return result, basis


def _normalise_channels(
    channels_value: Any, mappings: dict[str, dict[str, str]]
) -> dict[str, dict[str, Any]]:
    channels: dict[str, dict[str, Any]] = {}
    if channels_value is not None:
        raw_channels = _as_mapping(channels_value, "channels")
        for channel_id, raw in raw_channels.items():
            if not isinstance(channel_id, str) or not channel_id:
                raise TelemetryError(
                    "E_CONFIG_INVALID",
                    "通道 id 必须是非空字符串 / invalid channel id",
                    {"channel": channel_id},
                )
            if raw is None:
                raw = {}
            spec = _as_mapping(raw, f"channels.{channel_id}")
            channels[channel_id] = {
                "label": str(spec.get("label", channel_id)),
                "unit": str(spec.get("unit", "")),
            }
    for mapping in mappings.values():
        for channel_id in mapping.values():
            channels.setdefault(channel_id, {"label": channel_id, "unit": ""})
    return {key: channels[key] for key in sorted(channels)}


def _normalise_valid_ranges(
    source_id: str,
    raw: dict[str, Any],
    mapping: dict[str, str],
) -> dict[str, dict[str, float]]:
    result: dict[str, dict[str, float]] = {}
    raw_ranges = raw.get("valid_ranges")
    if raw_ranges is not None:
        ranges = _as_mapping(raw_ranges, f"sources.{source_id}.valid_ranges")
        for channel_id, raw_range in ranges.items():
            if isinstance(raw_range, (list, tuple)) and len(raw_range) == 2:
                low, high = raw_range
            elif isinstance(raw_range, dict):
                low = raw_range.get("min")
                high = raw_range.get("max")
            else:
                raise TelemetryError(
                    "E_CONFIG_INVALID",
                    "有效值区间必须是 [min,max] 或 {min,max} / invalid valid range",
                    {"source_id": source_id, "channel": channel_id},
                )
            low_f = _as_float(low, f"sources.{source_id}.valid_ranges.{channel_id}.min")
            high_f = _as_float(high, f"sources.{source_id}.valid_ranges.{channel_id}.max")
            if low_f > high_f:
                raise TelemetryError(
                    "E_CONFIG_INVALID",
                    "有效值区间下界大于上界 / min exceeds max",
                    {"source_id": source_id, "channel": channel_id, "min": low_f, "max": high_f},
                )
            result[str(channel_id)] = {"min": low_f, "max": high_f}
    if not result and ("valid_min" in raw or "valid_max" in raw):
        low_f = _as_float(raw.get("valid_min", float("-inf")), f"sources.{source_id}.valid_min")
        high_f = _as_float(raw.get("valid_max", float("inf")), f"sources.{source_id}.valid_max")
        if low_f > high_f:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "有效值区间下界大于上界 / min exceeds max",
                {"source_id": source_id, "min": low_f, "max": high_f},
            )
        for channel_id in sorted(set(mapping.values())):
            result[channel_id] = {"min": low_f, "max": high_f}
    if not result:
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "源缺少有效值区间 / source is missing valid ranges",
            {"source_id": source_id},
        )
    return {key: result[key] for key in sorted(result)}


def _normalise_interpolation(value: Any) -> dict[str, Any]:
    if value is None:
        value = {"default": "linear", "per_source": {}}
    if isinstance(value, str):
        value = {"default": value, "per_source": {}}
    spec = _as_mapping(value, "interpolation")
    default = spec.get("default", "linear")
    per_source = spec.get("per_source", spec.get("overrides", {}))
    if default not in INTERPOLATION_METHODS:
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "未知插值方式 / unsupported interpolation method",
            {"method": default, "allowed": sorted(INTERPOLATION_METHODS)},
        )
    per_source_map = _as_mapping(per_source, "interpolation.per_source")
    normalised_per_source: dict[str, str] = {}
    for source_id, method in per_source_map.items():
        if method not in INTERPOLATION_METHODS:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "未知插值方式 / unsupported interpolation method",
                {"source_id": source_id, "method": method, "allowed": sorted(INTERPOLATION_METHODS)},
            )
        normalised_per_source[str(source_id)] = str(method)
    return {
        "default": str(default),
        "per_source": {key: normalised_per_source[key] for key in sorted(normalised_per_source)},
    }


@dataclass(frozen=True)
class ConfigBundle:
    original: dict[str, Any]
    effective: dict[str, Any]
    schema_version: int
    effective_schema_version: int
    semantic: dict[str, Any]
    semantic_fingerprint: str
    config_fingerprint: str
    provenance: dict[str, Any]
    path: str | None = None

    @property
    def workspace_id(self) -> str:
        return str(self.original.get("workspace_id", "local-workspace"))

    @property
    def source_ids(self) -> list[str]:
        return sorted(self.effective["sources"])

    @property
    def channels(self) -> dict[str, dict[str, Any]]:
        return self.effective["channels"]

    @property
    def degraded(self) -> bool:
        return bool(self.provenance.get("degraded", False))

    def source(self, source_id: str) -> dict[str, Any]:
        try:
            return self.effective["sources"][source_id]
        except KeyError as exc:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "配置中不存在该源 / source is not configured",
                {"source_id": source_id, "configured": self.source_ids},
            ) from exc

    def interpolation_for(self, source_id: str) -> str:
        interpolation = self.effective["interpolation"]
        return interpolation["per_source"].get(source_id, interpolation["default"])

    def normalize_source_slice(self, source_id: str) -> dict[str, Any]:
        source = self.source(source_id)
        return {
            "channel_mapping": source["channel_mapping"],
            "valid_ranges": source["valid_ranges"],
            "quality_accept": source["quality_accept"],
        }

    def normalize_slice(self) -> dict[str, Any]:
        return {
            "grid": self.effective["grid"],
            "sources": {
                source_id: self.normalize_source_slice(source_id)
                for source_id in self.source_ids
            },
        }

    def resample_source_slice(self, source_id: str) -> dict[str, Any]:
        return {
            "interpolation": self.interpolation_for(source_id),
            "max_gap_ms": self.effective["resample"]["max_gap_ms"],
            "allow_extrapolation": self.effective["resample"]["allow_extrapolation"],
            "tolerance_ms": self.effective["tolerance_ms"],
        }

    def resample_slice(self) -> dict[str, Any]:
        return {
            "grid": self.effective["grid"],
            "tolerance_ms": self.effective["tolerance_ms"],
            "resample": self.effective["resample"],
            "interpolation": self.effective["interpolation"],
        }

    def fusion_slice(self) -> dict[str, Any]:
        return {
            "sources": {
                source_id: {
                    "priority": self.source(source_id)["priority"],
                    "channel_mapping": self.source(source_id)["channel_mapping"],
                    "valid_ranges": self.source(source_id)["valid_ranges"],
                }
                for source_id in self.source_ids
            },
            "fusion": self.effective["fusion"],
            "channels": self.effective["channels"],
        }

    def summary_slice(self) -> dict[str, Any]:
        return {"export": self.effective["export"]}

    def to_v2_dict(self) -> dict[str, Any]:
        grid = copy.deepcopy(self.effective["grid"])
        channels = copy.deepcopy(self.effective["channels"])
        sources: dict[str, Any] = {}
        for source_id in self.source_ids:
            source = self.source(source_id)
            sources[source_id] = {
                "priority": source["priority"],
                "channel_mapping": copy.deepcopy(source["channel_mapping"]),
                "valid_ranges": copy.deepcopy(source["valid_ranges"]),
                "quality_accept": list(source["quality_accept"]),
            }
        result: dict[str, Any] = {
            "schema_version": 2,
            "workspace_id": self.workspace_id,
            "grid": grid,
            "tolerance_ms": self.effective["tolerance_ms"],
            "resample": copy.deepcopy(self.effective["resample"]),
            "interpolation": copy.deepcopy(self.effective["interpolation"]),
            "channels": channels,
            "sources": sources,
            "fusion": copy.deepcopy(self.effective["fusion"]),
            "export": copy.deepcopy(self.effective["export"]),
        }
        if self.provenance:
            result["provenance"] = copy.deepcopy(self.provenance)
        return result

    def to_v2_yaml(self) -> str:
        return yaml.safe_dump(
            self.to_v2_dict(),
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
            width=1000,
        )


def _upgrade_v1(
    data: dict[str, Any], policy_override: dict[str, Any] | None
) -> tuple[dict[str, Any], dict[str, Any]]:
    compatibility = copy.deepcopy(data.get("compatibility") or {})
    if policy_override:
        compatibility.update(copy.deepcopy(policy_override))
    compatibility = _as_mapping(compatibility, "compatibility")
    if "tolerance_percent" not in data:
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "V1 配置缺少 tolerance_percent / V1 config lacks tolerance_percent",
            {},
        )
    basis_ms = compatibility.get("tolerance_basis_ms", 1000)
    rounding = str(compatibility.get("tolerance_rounding", "half_up"))
    tolerance_ms, tolerance_basis = _convert_percent_tolerance(
        data["tolerance_percent"], basis_ms, rounding
    )

    effective = copy.deepcopy(data)
    effective["schema_version"] = 2
    effective["tolerance_ms"] = tolerance_ms
    effective.pop("tolerance_percent", None)
    effective.pop("compatibility", None)

    channels = effective.get("channels") or {}
    if not isinstance(channels, dict):
        raise TelemetryError(
            "E_CONFIG_INVALID", "channels 必须是映射 / channels must be a mapping", {}
        )
    default_mapping = compatibility.get("default_channel_mapping")
    if default_mapping is None:
        default_mapping = {channel_id: channel_id for channel_id in channels}
    default_mapping = _as_mapping(default_mapping, "compatibility.default_channel_mapping")
    per_source_mapping = _as_mapping(
        compatibility.get("channel_mapping", {}),
        "compatibility.channel_mapping",
    )

    sources = _as_mapping(effective.get("sources"), "sources")
    mapping_evidence: dict[str, Any] = {}
    for source_id, raw_source in sources.items():
        source = _as_mapping(raw_source, f"sources.{source_id}")
        source_mapping = per_source_mapping.get(source_id)
        mapping_rule = "source_override"
        if source_mapping is None:
            source_mapping = default_mapping
            mapping_rule = (
                "default_channel_mapping"
                if "default_channel_mapping" in compatibility
                else "channel_identity_fallback"
            )
        source_mapping = _as_mapping(
            source_mapping, f"compatibility.channel_mapping.{source_id}"
        )
        normalised_mapping = {
            str(raw_channel): str(channel)
            for raw_channel, channel in sorted(source_mapping.items())
        }
        if not normalised_mapping:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "V1 通道映射补齐结果为空 / inferred V1 channel mapping is empty",
                {"source_id": source_id},
            )
        source["channel_mapping"] = normalised_mapping
        mapping_evidence[str(source_id)] = {
            "rule": mapping_rule,
            "mapping": normalised_mapping,
        }

    provenance = {
        "upgraded_from": 1,
        "degraded": True,
        "conversion": {
            "tolerance": tolerance_basis,
            "channel_mapping": mapping_evidence,
        },
        "notes": [
            "V1 整数百分比容差已换算为毫秒 / integer percent tolerance converted to milliseconds",
            "缺失的源级通道映射已按兼容规则补齐 / missing source channel maps were filled",
        ],
    }
    effective["provenance"] = provenance
    return effective, provenance


def _normalise_effective(data: dict[str, Any]) -> dict[str, Any]:
    grid_raw = _as_mapping(data.get("grid"), "grid")
    step_ms = _as_int(grid_raw.get("step_ms"), "grid.step_ms")
    start_ms = _as_int(grid_raw.get("start_ms"), "grid.start_ms")
    end_ms = _as_int(grid_raw.get("end_ms"), "grid.end_ms")
    if end_ms < start_ms:
        raise TelemetryError(
            "E_GRID_INVALID",
            "网格结束时间早于起始时间 / grid end precedes start",
            {"start_ms": start_ms, "end_ms": end_ms},
        )
    tolerance_ms = _as_int(data.get("tolerance_ms"), "tolerance_ms")
    if tolerance_ms <= 0:
        raise TelemetryError(
            "E_GRID_INVALID",
            "对齐容差必须为正 / alignment tolerance must be positive",
            {"tolerance_ms": tolerance_ms},
        )
    if step_ms <= 0:
        raise TelemetryError(
            "E_GRID_INVALID",
            "网格步长必须为正 / grid step must be positive",
            {"step_ms": step_ms, "tolerance_ms": tolerance_ms},
        )
    if step_ms > tolerance_ms:
        raise TelemetryError(
            "E_GRID_INVALID",
            "网格步长超过对齐容差 / grid step exceeds alignment tolerance",
            {"step_ms": step_ms, "tolerance_ms": tolerance_ms},
        )

    resample_raw = _as_mapping(data.get("resample", {}), "resample")
    max_gap_ms = _as_int(resample_raw.get("max_gap_ms", step_ms), "resample.max_gap_ms", minimum=0)
    allow_extrapolation = resample_raw.get("allow_extrapolation", False)
    if not isinstance(allow_extrapolation, bool):
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "allow_extrapolation 必须是布尔值 / must be boolean",
            {"allow_extrapolation": allow_extrapolation},
        )

    interpolation = _normalise_interpolation(data.get("interpolation"))
    sources_raw = _as_mapping(data.get("sources"), "sources")
    if not sources_raw:
        raise TelemetryError("E_CONFIG_INVALID", "配置至少需要一个源 / at least one source is required", {})

    raw_mappings: dict[str, dict[str, str]] = {}
    for source_id, raw_source in sources_raw.items():
        source = _as_mapping(raw_source, f"sources.{source_id}")
        mapping = _as_mapping(
            source.get("channel_mapping"), f"sources.{source_id}.channel_mapping"
        )
        if not mapping:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "V2 源通道映射不能为空 / V2 channel mapping cannot be empty",
                {"source_id": source_id},
            )
        normalised_mapping: dict[str, str] = {}
        for raw_channel, channel_id in sorted(mapping.items()):
            if not isinstance(raw_channel, str) or not raw_channel:
                raise TelemetryError(
                    "E_CONFIG_INVALID", "原始通道名无效 / invalid raw channel", {"source_id": source_id}
                )
            if not isinstance(channel_id, str) or not channel_id:
                raise TelemetryError(
                    "E_CONFIG_INVALID", "目标通道名无效 / invalid canonical channel", {"source_id": source_id}
                )
            normalised_mapping[raw_channel] = channel_id
        raw_mappings[str(source_id)] = normalised_mapping

    channels = _normalise_channels(data.get("channels"), raw_mappings)
    sources: dict[str, Any] = {}
    for source_id, raw_source in sources_raw.items():
        source = _as_mapping(raw_source, f"sources.{source_id}")
        priority = _as_int(source.get("priority"), f"sources.{source_id}.priority")
        mapping = raw_mappings[str(source_id)]
        valid_ranges = _normalise_valid_ranges(str(source_id), source, mapping)
        missing_ranges = sorted(set(mapping.values()) - set(valid_ranges))
        if missing_ranges:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "源通道缺少有效值区间 / mapped channels lack valid ranges",
                {"source_id": source_id, "channels": missing_ranges},
            )
        quality_accept = source.get("quality_accept", ["good"])
        if not isinstance(quality_accept, list) or not all(
            isinstance(item, str) for item in quality_accept
        ):
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "quality_accept 必须是字符串数组 / must be an array of strings",
                {"source_id": source_id},
            )
        sources[str(source_id)] = {
            "priority": priority,
            "channel_mapping": mapping,
            "valid_ranges": valid_ranges,
            "quality_accept": list(quality_accept),
        }

    fusion_raw = _as_mapping(data.get("fusion", {}), "fusion")
    mode = str(fusion_raw.get("mode", "weighted_mean"))
    if mode not in FUSION_MODES:
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "未知融合模式 / unsupported fusion mode",
            {"mode": mode, "allowed": sorted(FUSION_MODES)},
        )
    tie_break = str(fusion_raw.get("tie_break", "source_id_lexicographic"))
    if tie_break not in TIE_BREAK_RULES:
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "未知并列仲裁规则 / unsupported tie-break rule",
            {"tie_break": tie_break, "allowed": sorted(TIE_BREAK_RULES)},
        )
    weights_raw = fusion_raw.get("weights", {})
    weights_map = _as_mapping(weights_raw, "fusion.weights")
    weights: dict[str, float] = {}
    for source_id in sorted(sources):
        weight = _as_float(weights_map.get(source_id, 1.0), f"fusion.weights.{source_id}")
        if weight < 0:
            raise TelemetryError(
                "E_CONFIG_INVALID",
                "融合权重不能为负 / fusion weight cannot be negative",
                {"source_id": source_id, "weight": weight},
            )
        weights[source_id] = weight
    if not any(weight > 0 for weight in weights.values()):
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "至少一个融合权重必须为正 / at least one weight must be positive",
            {"weights": weights},
        )

    export_raw = _as_mapping(data.get("export", {}), "export")
    export = {
        "title": str(export_raw.get("title", "多源时序对齐摘要 / Multi-source Alignment Summary")),
        "include_points": bool(export_raw.get("include_points", True)),
    }

    provenance = copy.deepcopy(data.get("provenance", {}))
    effective = {
        "grid": {"step_ms": step_ms, "start_ms": start_ms, "end_ms": end_ms},
        "tolerance_ms": tolerance_ms,
        "resample": {
            "max_gap_ms": max_gap_ms,
            "allow_extrapolation": allow_extrapolation,
        },
        "interpolation": interpolation,
        "channels": channels,
        "sources": {key: sources[key] for key in sorted(sources)},
        "fusion": {
            "mode": mode,
            "tie_break": tie_break,
            "weights": {key: weights[key] for key in sorted(weights)},
        },
        "export": export,
        "provenance": provenance,
    }
    # Interpolation overrides for unknown sources are a config mistake, not a hidden fallback.
    unknown = sorted(set(interpolation["per_source"]) - set(effective["sources"]))
    if unknown:
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "插值覆盖引用了未知源 / interpolation override references unknown sources",
            {"unknown_sources": unknown},
        )
    return effective


def parse_config(
    data: dict[str, Any],
    *,
    policy_override: dict[str, Any] | None = None,
    path: str | None = None,
) -> ConfigBundle:
    if not isinstance(data, dict):
        raise TelemetryError("E_CONFIG_INVALID", "配置根必须是映射 / config root must be mapping", {})
    original = copy.deepcopy(data)
    schema_version = data.get("schema_version")
    if isinstance(schema_version, bool) or not isinstance(schema_version, int):
        raise TelemetryError(
            "E_CONFIG_INVALID", "schema_version 必须是整数 / must be an integer", {"schema_version": schema_version}
        )
    if schema_version == 1:
        upgraded, provenance = _upgrade_v1(data, policy_override)
        effective = _normalise_effective(upgraded)
    elif schema_version == 2:
        effective = _normalise_effective(copy.deepcopy(data))
        provenance = copy.deepcopy(effective.get("provenance", {}))
    else:
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "仅支持结构版本 1 或 2 / only schema versions 1 and 2 are supported",
            {"schema_version": schema_version},
        )

    semantic = {
        "grid": effective["grid"],
        "tolerance_ms": effective["tolerance_ms"],
        "resample": effective["resample"],
        "interpolation": effective["interpolation"],
        "channels": effective["channels"],
        "sources": effective["sources"],
        "fusion": effective["fusion"],
        "export": effective["export"],
    }
    semantic_fingerprint = digest_obj(semantic)
    config_fingerprint = digest_obj(original)
    return ConfigBundle(
        original=original,
        effective=effective,
        schema_version=schema_version,
        effective_schema_version=2,
        semantic=semantic,
        semantic_fingerprint=semantic_fingerprint,
        config_fingerprint=config_fingerprint,
        provenance=provenance,
        path=path,
    )


def load_config(
    path: Path | str,
    *,
    policy_override: dict[str, Any] | None = None,
) -> ConfigBundle:
    config_path = Path(path)
    try:
        text = config_path.read_text(encoding="utf-8")
        raw = yaml.safe_load(text)
    except FileNotFoundError as exc:
        raise TelemetryError(
            "E_NOT_FOUND", "配置文件不存在 / config file not found", {"path": str(config_path)}
        ) from exc
    except yaml.YAMLError as exc:
        raise TelemetryError(
            "E_CONFIG_INVALID",
            "YAML 解析失败 / invalid YAML",
            {"path": str(config_path), "reason": str(exc)},
        ) from exc
    return parse_config(raw, policy_override=policy_override, path=str(config_path))


def config_to_yaml(config: ConfigBundle) -> str:
    return config.to_v2_yaml()


def semantic_equal(left: ConfigBundle, right: ConfigBundle) -> bool:
    return canonical_json_text(left.semantic) == canonical_json_text(right.semantic)

