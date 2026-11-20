"""YAML configuration parsing and canonical fingerprints."""

from __future__ import annotations

import math
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .canonical import canonical_digest
from .errors import E_INVALID_CONFIG, AuditError

VALID_MODES = {"symmetric": "symmetric", "sym": "symmetric", "asymmetric": "asymmetric", "asym": "asymmetric", "affine": "asymmetric"}
VALID_ZERO_POINT_POLICIES = {"exact", "zero", "clamp"}
VALID_BUDGET_METRICS = {"relative_l2", "relative_max", "absolute_max"}


@dataclass(frozen=True)
class LayerQuantConfig:
    name: str
    bits: int
    mode: str
    zero_point: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "bits": self.bits,
            "mode": self.mode,
            "zero_point": self.zero_point,
        }


@dataclass(frozen=True)
class QuantConfig:
    layers: tuple[LayerQuantConfig, ...]
    calibration_ratio: float
    per_layer_budget: dict[str, float]
    total_budget: float
    budget_metric: str
    closure_tolerance: float
    upstream_share_threshold: float
    verify_incremental: bool
    source_path: str | None = None

    def layer_map(self) -> dict[str, LayerQuantConfig]:
        return {layer.name: layer for layer in self.layers}

    def normalized(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "calibration": {"ratio": self.calibration_ratio},
            "quantization": {
                "layers": {layer.name: layer.as_dict() for layer in self.layers}
            },
            "budget": {
                "metric": self.budget_metric,
                "per_layer": dict(sorted(self.per_layer_budget.items())),
                "total": self.total_budget,
            },
            "attribution": {
                "closure_tolerance": self.closure_tolerance,
                "upstream_share_threshold": self.upstream_share_threshold,
            },
            "runtime": {"verify_incremental": self.verify_incremental},
        }

    @property
    def fingerprint(self) -> str:
        return canonical_digest(self.normalized())

    @property
    def budget_fingerprint(self) -> str:
        return canonical_digest(self.normalized()["budget"])


def load_config(path: str | Path, layer_names: Iterable[str]) -> QuantConfig:
    source = Path(path)
    try:
        with source.open("r", encoding="utf-8") as handle:
            raw = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError) as exc:
        raise AuditError(E_INVALID_CONFIG, f"cannot read YAML config: {exc}", path=str(source)) from exc
    if not isinstance(raw, dict):
        raise AuditError(E_INVALID_CONFIG, "configuration root must be a mapping", path=str(source))
    return parse_config(raw, layer_names, source_path=str(source))


def parse_config(raw: dict[str, Any], layer_names: Iterable[str], source_path: str | None = None) -> QuantConfig:
    names = list(layer_names)
    if not names:
        raise AuditError(E_INVALID_CONFIG, "layer manifest is empty")

    quant = raw.get("quantization", raw.get("quant", {}))
    if not isinstance(quant, dict):
        raise AuditError(E_INVALID_CONFIG, "quantization must be a mapping")
    defaults = quant.get("default", {})
    if not isinstance(defaults, dict):
        raise AuditError(E_INVALID_CONFIG, "quantization.default must be a mapping")
    base = {**quant, **defaults}
    base_bits = base.get("bits", 8)
    base_mode = base.get("mode", "symmetric")
    base_zero = base.get(
        "zero_point",
        base.get("zero-point", base.get("zero_point_strategy", base.get("zero-point-strategy", "exact"))),
    )

    layer_overrides: dict[str, dict[str, Any]] = {}
    layers_section = quant.get("layers", {})
    if isinstance(layers_section, list):
        for entry in layers_section:
            if not isinstance(entry, dict) or "name" not in entry:
                raise AuditError(E_INVALID_CONFIG, "each quantization.layers list entry needs name")
            name = str(entry["name"])
            if name in layer_overrides:
                raise AuditError(E_INVALID_CONFIG, f"duplicate layer config: {name}", layer=name)
            layer_overrides[name] = {k: v for k, v in entry.items() if k != "name"}
    elif isinstance(layers_section, dict):
        for name, entry in layers_section.items():
            if not isinstance(entry, dict):
                raise AuditError(E_INVALID_CONFIG, f"layer config must be a mapping: {name}", layer=str(name))
            layer_overrides[str(name)] = entry
    else:
        raise AuditError(E_INVALID_CONFIG, "quantization.layers must be a mapping or list")

    unknown = sorted(set(layer_overrides) - set(names))
    if unknown:
        raise AuditError(E_INVALID_CONFIG, "configuration references unknown layers", layers=unknown)

    budget_raw = raw.get("budget", raw.get("error_budget", {}))
    if not isinstance(budget_raw, dict):
        raise AuditError(E_INVALID_CONFIG, "budget must be a mapping")
    per_layer_budget_raw = budget_raw.get("per_layer", budget_raw.get("layers", {}))
    if not isinstance(per_layer_budget_raw, dict):
        raise AuditError(E_INVALID_CONFIG, "budget.per_layer must be a mapping")

    attribution_raw = raw.get("attribution", {})
    if not isinstance(attribution_raw, dict):
        raise AuditError(E_INVALID_CONFIG, "attribution must be a mapping")
    runtime_raw = raw.get("runtime", {})
    if not isinstance(runtime_raw, dict):
        raise AuditError(E_INVALID_CONFIG, "runtime must be a mapping")

    calibration_raw = raw.get("calibration", {})
    if not isinstance(calibration_raw, dict):
        raise AuditError(E_INVALID_CONFIG, "calibration must be a mapping")
    ratio = _positive_float(
        raw.get(
            "calibration_ratio",
            raw.get(
                "calibration_sample_ratio",
                calibration_raw.get("ratio", calibration_raw.get("sample_ratio", 1.0)),
            ),
        ),
        "calibration_ratio",
    )
    if ratio > 1.0:
        raise AuditError(E_INVALID_CONFIG, "calibration_ratio must be in (0, 1]", value=ratio)

    total_budget = _positive_float(
        budget_raw.get("total", budget_raw.get("total_budget")),
        "budget.total",
    )
    metric = str(budget_raw.get("metric", "relative_l2"))
    if metric not in VALID_BUDGET_METRICS:
        raise AuditError(E_INVALID_CONFIG, f"unsupported budget metric: {metric}", metric=metric)
    closure_tolerance = _nonnegative_float(
        attribution_raw.get("closure_tolerance", 0.25), "attribution.closure_tolerance"
    )
    upstream_threshold = _nonnegative_float(
        attribution_raw.get("upstream_share_threshold", 0.5),
        "attribution.upstream_share_threshold",
    )
    if upstream_threshold > 1.0:
        raise AuditError(E_INVALID_CONFIG, "upstream_share_threshold must be <= 1")

    layers: list[LayerQuantConfig] = []
    budgets: dict[str, float] = {}
    for name in names:
        entry = {**base, **layer_overrides.get(name, {})}
        bits = _parse_bits(entry.get("bits", base_bits), name)
        mode_raw = str(entry.get("mode", base_mode)).strip().lower()
        mode = VALID_MODES.get(mode_raw)
        if mode is None:
            raise AuditError(E_INVALID_CONFIG, f"unsupported quantization mode: {mode_raw}", layer=name)
        zero_raw = str(
            entry.get(
                "zero_point",
                entry.get(
                    "zero-point",
                    entry.get("zero_point_strategy", entry.get("zero-point-strategy", base_zero)),
                ),
            )
        ).strip().lower()
        zero_point = "clamp" if zero_raw == "saturating" else zero_raw
        if zero_point not in VALID_ZERO_POINT_POLICIES:
            raise AuditError(E_INVALID_CONFIG, f"unsupported zero point policy: {zero_raw}", layer=name)
        layers.append(LayerQuantConfig(name=name, bits=bits, mode=mode, zero_point=zero_point))

        budget_value = entry.get(
            "error_budget",
            entry.get("budget", per_layer_budget_raw.get(name)),
        )
        if budget_value is None:
            raise AuditError(E_INVALID_CONFIG, f"missing error budget for layer {name}", layer=name)
        budgets[name] = _positive_float(budget_value, f"budget[{name}]")

    unknown_budgets = sorted(set(per_layer_budget_raw) - set(names))
    if unknown_budgets:
        raise AuditError(E_INVALID_CONFIG, "budget references unknown layers", layers=unknown_budgets)

    verify_incremental = bool(runtime_raw.get("verify_incremental", True))
    return QuantConfig(
        layers=tuple(layers),
        calibration_ratio=ratio,
        per_layer_budget=budgets,
        total_budget=total_budget,
        budget_metric=metric,
        closure_tolerance=closure_tolerance,
        upstream_share_threshold=upstream_threshold,
        verify_incremental=verify_incremental,
        source_path=source_path,
    )


def _parse_bits(value: Any, layer: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) != value:
        raise AuditError(E_INVALID_CONFIG, "bits must be an integer", layer=layer, value=value)
    bits = int(value)
    if bits < 2 or bits > 32:
        raise AuditError(E_INVALID_CONFIG, "bits must be between 2 and 32", layer=layer, value=bits)
    return bits


def _positive_float(value: Any, field: str) -> float:
    number = _number(value, field)
    if number <= 0:
        raise AuditError(E_INVALID_CONFIG, f"{field} must be positive", field=field, value=number)
    return number


def _nonnegative_float(value: Any, field: str) -> float:
    number = _number(value, field)
    if number < 0:
        raise AuditError(E_INVALID_CONFIG, f"{field} must be non-negative", field=field, value=number)
    return number


def _number(value: Any, field: str) -> float:
    if value is None or isinstance(value, bool):
        raise AuditError(E_INVALID_CONFIG, f"missing or invalid numeric field: {field}", field=field)
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise AuditError(E_INVALID_CONFIG, f"invalid numeric field: {field}", field=field, value=value) from exc
    if not math.isfinite(number):
        raise AuditError(E_INVALID_CONFIG, f"non-finite numeric field: {field}", field=field)
    return number

