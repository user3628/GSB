from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

from .errors import FusionError
from .util import canonical_json_bytes, ensure_mapping, read_json, sha256_bytes

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")


def _as_int(value: Any, field_name: str, *, layer: str | None = None, index: int | None = None, minimum: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise FusionError.invalid_param(layer or "<manifest>", index, field_name, "must be an integer", value=repr(value))
    if minimum is not None and value < minimum:
        raise FusionError.invalid_param(layer or "<manifest>", index, field_name, f"must be >= {minimum}", value=value)
    return value


def _as_float(value: Any, field_name: str, *, layer: str | None = None, index: int | None = None) -> float:
    if isinstance(value, bool):
        raise FusionError.invalid_param(layer or "<manifest>", index, field_name, "must be numeric", value=repr(value))
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise FusionError.invalid_param(layer or "<manifest>", index, field_name, "must be numeric", value=repr(value))
    if not math.isfinite(result):
        raise FusionError.invalid_param(layer or "<manifest>", index, field_name, "must be finite", value=repr(value))
    return result


def _as_name(value: Any, field_name: str, *, layer: str | None = None, index: int | None = None) -> str:
    if not isinstance(value, str) or not value or not _NAME_RE.fullmatch(value):
        raise FusionError.invalid_param(layer or "<manifest>", index, field_name, "must match [A-Za-z0-9][A-Za-z0-9_.-]*", value=repr(value))
    return value


def _as_pair(value: Any, field_name: str, *, layer: str, minimum: int = 1) -> tuple[int, int]:
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise FusionError.invalid_param(layer, None, field_name, "must be a two-element integer list", value=repr(value))
    return (_as_int(value[0], f"{field_name}[0]", layer=layer, minimum=minimum), _as_int(value[1], f"{field_name}[1]", layer=layer, minimum=minimum))


@dataclass(frozen=True)
class LateralSpec:
    kernel_size: tuple[int, int]
    stride: tuple[int, int]
    out_channels: int
    padding: str = "same"
    weight_scale: float = 0.75
    kernel_seed: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "kernel_size": list(self.kernel_size),
            "stride": list(self.stride),
            "out_channels": self.out_channels,
            "padding": self.padding,
            "weight_scale": self.weight_scale,
            "kernel_seed": self.kernel_seed,
        }


@dataclass(frozen=True)
class AttentionSpec:
    weights: tuple[float, ...]
    normalization: str = "sum_to_one"

    @property
    def raw_sum(self) -> float:
        return math.fsum(self.weights)

    def to_dict(self) -> dict[str, Any]:
        return {"weights": list(self.weights), "normalization": self.normalization}


@dataclass(frozen=True)
class FusionSpec:
    coeffs: Mapping[str, float]
    mode: str = "weighted_sum"

    def to_dict(self) -> dict[str, Any]:
        return {"coeffs": dict(sorted(self.coeffs.items())), "mode": self.mode}


@dataclass(frozen=True)
class UpsampleSpec:
    scale: int
    mode: str = "nearest"
    align_corners: bool = False
    max_tail: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "scale": self.scale,
            "mode": self.mode,
            "align_corners": self.align_corners,
            "max_tail": self.max_tail,
        }


@dataclass(frozen=True)
class QuantizationSpec:
    bits: int
    scale_override: float | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"bits": self.bits}
        if self.scale_override is not None:
            payload["scale_override"] = self.scale_override
        return payload


@dataclass(frozen=True)
class LayerSpec:
    name: str
    input: str
    expected_output_shape: tuple[int, int, int, int]
    allowed_range: tuple[float, float]
    lateral: LateralSpec
    attention: AttentionSpec
    fusion: FusionSpec
    quantization: QuantizationSpec
    reference: str | None = None
    upsample: UpsampleSpec | None = None
    manifest_index: int = 0

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "input": self.input,
            "expected_output_shape": list(self.expected_output_shape),
            "allowed_range": list(self.allowed_range),
            "lateral": self.lateral.to_dict(),
            "attention": self.attention.to_dict(),
            "fusion": self.fusion.to_dict(),
            "quantization": self.quantization.to_dict(),
        }
        if self.reference is not None:
            payload["reference"] = self.reference
        if self.upsample is not None:
            payload["upsample"] = self.upsample.to_dict()
        return payload


@dataclass(frozen=True)
class SensitivitySpec:
    amplitude: float = 1e-3
    tolerance: float = 0.25
    first_order_probe: float = 1e-4

    def to_dict(self) -> dict[str, Any]:
        return {
            "amplitude": self.amplitude,
            "tolerance": self.tolerance,
            "first_order_probe": self.first_order_probe,
        }


@dataclass(frozen=True)
class TaskSpec:
    id: str
    seed: int
    layers: tuple[LayerSpec, ...]
    sensitivity: SensitivitySpec = field(default_factory=SensitivitySpec)
    final_reference: str | None = None
    schema_version: int = 1

    @property
    def processing_layers(self) -> tuple[LayerSpec, ...]:
        return tuple(reversed(self.layers))

    @property
    def deepest_layer(self) -> LayerSpec:
        return self.layers[-1]

    @property
    def finest_layer(self) -> LayerSpec:
        return self.layers[0]

    def layer_by_name(self, name: str) -> LayerSpec:
        for layer in self.layers:
            if layer.name == name:
                return layer
        raise FusionError.invalid_stage(f"unknown layer {name!r}", layer=name, task_id=self.id)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "id": self.id,
            "seed": self.seed,
            "schema_version": self.schema_version,
            "layers": [layer.to_dict() for layer in self.layers],
            "sensitivity": self.sensitivity.to_dict(),
        }
        if self.final_reference is not None:
            payload["final_reference"] = self.final_reference
        return payload

    def config_fingerprint(self) -> str:
        return sha256_bytes(canonical_json_bytes(self.to_dict()))


@dataclass(frozen=True)
class Manifest:
    schema_version: int
    tasks: tuple[TaskSpec, ...]
    source_path: Path | None = None

    def by_id(self, task_id: str) -> TaskSpec:
        for task in self.tasks:
            if task.id == task_id:
                return task
        raise FusionError.invalid_stage(
            f"task {task_id!r} is not present in manifest",
            details={"task_id": task_id, "available_tasks": [task.id for task in self.tasks]},
        )

    @classmethod
    def load(cls, path: str | Path) -> "Manifest":
        path = Path(path)
        try:
            raw = read_json(path)
        except FileNotFoundError:
            raise FusionError("E_INVALID_STAGE", f"manifest not found: {path}", details={"path": str(path)})
        except Exception as exc:  # noqa: BLE001
            raise FusionError("E_INVALID_PARAM", f"cannot parse manifest: {exc}", details={"path": str(path)})
        if not isinstance(raw, Mapping):
            raise FusionError("E_INVALID_PARAM", "manifest root must be an object")
        schema_version = _as_int(raw.get("schema_version", 1), "schema_version", minimum=1)
        raw_tasks = raw.get("tasks")
        if raw_tasks is None:
            raw_tasks = [raw]
        if not isinstance(raw_tasks, list) or not raw_tasks:
            raise FusionError("E_INVALID_PARAM", "manifest must contain a non-empty tasks list")
        tasks = tuple(_parse_task(item, schema_version) for item in raw_tasks)
        ids = [task.id for task in tasks]
        if len(ids) != len(set(ids)):
            raise FusionError("E_INVALID_PARAM", "task ids must be unique", details={"task_ids": ids})
        return cls(schema_version=schema_version, tasks=tasks, source_path=path.resolve())


def _parse_task(raw: Any, schema_version: int) -> TaskSpec:
    raw = ensure_mapping(raw, "task")
    task_id = _as_name(raw.get("id"), "task.id")
    seed = _as_int(raw.get("seed", 0), "task.seed", layer=task_id)
    raw_layers = raw.get("layers")
    if not isinstance(raw_layers, list) or len(raw_layers) < 1:
        raise FusionError("E_INVALID_PARAM", "task.layers must be a non-empty list", details={"task_id": task_id})
    layers: list[LayerSpec] = []
    for index, raw_layer in enumerate(raw_layers):
        layer = _parse_layer(raw_layer, index, len(raw_layers))
        layers.append(layer)
    names = [layer.name for layer in layers]
    if len(names) != len(set(names)):
        raise FusionError("E_INVALID_PARAM", "layer names must be unique", details={"task_id": task_id, "layers": names})
    sens_raw = raw.get("sensitivity") or {}
    sens_raw = ensure_mapping(sens_raw, "task.sensitivity")
    sensitivity = SensitivitySpec(
        amplitude=_as_float(sens_raw.get("amplitude", 1e-3), "sensitivity.amplitude", layer=task_id),
        tolerance=_as_float(sens_raw.get("tolerance", 0.25), "sensitivity.tolerance", layer=task_id),
        first_order_probe=_as_float(sens_raw.get("first_order_probe", 1e-4), "sensitivity.first_order_probe", layer=task_id),
    )
    if sensitivity.amplitude <= 0:
        raise FusionError.invalid_param(task_id, None, "sensitivity.amplitude", "must be > 0")
    if sensitivity.tolerance < 0:
        raise FusionError.invalid_param(task_id, None, "sensitivity.tolerance", "must be >= 0")
    if sensitivity.first_order_probe <= 0:
        raise FusionError.invalid_param(task_id, None, "sensitivity.first_order_probe", "must be > 0")
    final_reference = raw.get("final_reference")
    if final_reference is not None and not isinstance(final_reference, str):
        raise FusionError.invalid_param(task_id, None, "final_reference", "must be a string")
    return TaskSpec(
        id=task_id,
        seed=seed,
        layers=tuple(layers),
        sensitivity=sensitivity,
        final_reference=final_reference,
        schema_version=schema_version,
    )


def _parse_layer(raw: Any, index: int, total: int) -> LayerSpec:
    raw = ensure_mapping(raw, f"layers[{index}]")
    name = _as_name(raw.get("name"), "name", index=index)
    source = raw.get("input")
    if not isinstance(source, str) or not source:
        raise FusionError.invalid_param(name, None, "input", "must be a non-empty relative path")
    expected_raw = raw.get("expected_output_shape")
    if not isinstance(expected_raw, (list, tuple)) or len(expected_raw) != 4:
        raise FusionError.invalid_param(name, None, "expected_output_shape", "must have four dimensions")
    expected = tuple(_as_int(v, f"expected_output_shape[{i}]", layer=name, minimum=1) for i, v in enumerate(expected_raw))
    range_raw = raw.get("allowed_range")
    if not isinstance(range_raw, (list, tuple)) or len(range_raw) != 2:
        raise FusionError.invalid_param(name, None, "allowed_range", "must be [low, high]")
    low = _as_float(range_raw[0], "allowed_range[0]", layer=name)
    high = _as_float(range_raw[1], "allowed_range[1]", layer=name)
    if low > high:
        raise FusionError.invalid_param(name, None, "allowed_range", "low must be <= high", low=low, high=high)

    lat_raw = ensure_mapping(raw.get("lateral"), "lateral")
    lateral = LateralSpec(
        kernel_size=_as_pair(lat_raw.get("kernel_size"), "lateral.kernel_size", layer=name),
        stride=_as_pair(lat_raw.get("stride"), "lateral.stride", layer=name),
        out_channels=_as_int(lat_raw.get("out_channels"), "lateral.out_channels", layer=name, minimum=1),
        padding=str(lat_raw.get("padding", "same")),
        weight_scale=_as_float(lat_raw.get("weight_scale", 0.75), "lateral.weight_scale", layer=name),
        kernel_seed=None if lat_raw.get("kernel_seed") is None else _as_int(lat_raw.get("kernel_seed"), "lateral.kernel_seed", layer=name),
    )
    if lateral.padding not in {"same", "valid"}:
        raise FusionError.invalid_param(name, None, "lateral.padding", "must be 'same' or 'valid'")
    if lateral.weight_scale <= 0:
        raise FusionError.invalid_param(name, None, "lateral.weight_scale", "must be > 0")
    if expected[1] != lateral.out_channels:
        raise FusionError.invalid_param(
            name,
            None,
            "expected_output_shape[1]",
            "must equal lateral.out_channels",
            expected_channels=expected[1],
            out_channels=lateral.out_channels,
        )

    attn_raw = ensure_mapping(raw.get("attention"), "attention")
    weights_raw = attn_raw.get("weights")
    if not isinstance(weights_raw, (list, tuple)) or not weights_raw:
        raise FusionError.invalid_param(name, None, "attention.weights", "must be a non-empty list")
    weights: list[float] = []
    for wi, value in enumerate(weights_raw):
        weights.append(_as_float(value, "attention.weights", layer=name, index=wi))
    normalization = str(attn_raw.get("normalization", "sum_to_one"))
    if normalization not in {"sum_to_one", "l1"}:
        raise FusionError.invalid_param(name, None, "attention.normalization", "must be 'sum_to_one' or 'l1'")
    attention = AttentionSpec(weights=tuple(weights), normalization=normalization)

    fusion_raw = ensure_mapping(raw.get("fusion"), "fusion")
    coeffs_raw = fusion_raw.get("coeffs")
    if isinstance(coeffs_raw, (list, tuple)):
        if len(coeffs_raw) == 1:
            coeffs_raw = {"lateral": coeffs_raw[0]}
        elif len(coeffs_raw) == 2:
            coeffs_raw = {"lateral": coeffs_raw[0], "topdown": coeffs_raw[1]}
        else:
            raise FusionError.invalid_param(name, None, "fusion.coeffs", "list form must have one or two entries")
    coeffs_map = ensure_mapping(coeffs_raw, "fusion.coeffs")
    coeffs: dict[str, float] = {}
    for ci, (key, value) in enumerate(coeffs_map.items()):
        if key not in {"lateral", "topdown"}:
            raise FusionError.invalid_param(name, ci, "fusion.coeffs", f"unknown coefficient {key!r}")
        coeffs[key] = _as_float(value, "fusion.coeffs", layer=name, index=ci)
    if not coeffs:
        raise FusionError.invalid_param(name, 0, "fusion.coeffs", "must not be empty")
    if all(value == 0.0 for value in coeffs.values()):
        raise FusionError.invalid_param(name, 0, "fusion.coeffs", "all coefficients are zero", coefficients=coeffs)
    is_deepest = index == total - 1
    required = {"lateral"} if is_deepest else {"lateral", "topdown"}
    missing = required.difference(coeffs)
    extra = set(coeffs).difference(required)
    if missing:
        raise FusionError.invalid_param(name, None, "fusion.coeffs", "missing required coefficients", missing=sorted(missing))
    if extra:
        raise FusionError.invalid_param(name, None, "fusion.coeffs", "coefficient is invalid for this position", extra=sorted(extra))
    mode = str(fusion_raw.get("mode", "weighted_sum"))
    if mode != "weighted_sum":
        raise FusionError.invalid_param(name, None, "fusion.mode", "only 'weighted_sum' is supported")
    fusion = FusionSpec(coeffs=coeffs, mode=mode)

    upsample_raw = raw.get("upsample")
    upsample = None
    if upsample_raw is not None:
        upsample_raw = ensure_mapping(upsample_raw, "upsample")
        align = upsample_raw.get("align_corners", False)
        if not isinstance(align, bool):
            raise FusionError.invalid_param(name, None, "upsample.align_corners", "must be boolean")
        upsample = UpsampleSpec(
            scale=_as_int(upsample_raw.get("scale"), "upsample.scale", layer=name, minimum=1),
            mode=str(upsample_raw.get("mode", "nearest")),
            align_corners=align,
            max_tail=_as_int(upsample_raw.get("max_tail", 1), "upsample.max_tail", layer=name, minimum=0),
        )
        if upsample.mode not in {"nearest", "bilinear"}:
            raise FusionError.invalid_param(name, None, "upsample.mode", "must be 'nearest' or 'bilinear'")
    if not is_deepest and upsample is None:
        raise FusionError.invalid_param(name, None, "upsample", "is required for every layer that receives a top-down connection")
    if is_deepest and upsample is not None:
        raise FusionError.invalid_param(name, None, "upsample", "must be omitted for the deepest layer")

    quant_raw = ensure_mapping(raw.get("quantization"), "quantization")
    bits = _as_int(quant_raw.get("bits"), "quantization.bits", layer=name, minimum=2)
    if bits > 24:
        raise FusionError.invalid_param(name, None, "quantization.bits", "must be <= 24")
    scale_override = quant_raw.get("scale_override")
    if scale_override is not None:
        scale_override = _as_float(scale_override, "quantization.scale_override", layer=name)
        if scale_override <= 0:
            raise FusionError.invalid_param(name, None, "quantization.scale_override", "must be > 0")
    quantization = QuantizationSpec(bits=bits, scale_override=scale_override)

    reference = raw.get("reference")
    if reference is not None and not isinstance(reference, str):
        raise FusionError.invalid_param(name, None, "reference", "must be a relative path")
    return LayerSpec(
        name=name,
        input=source,
        expected_output_shape=expected,
        allowed_range=(low, high),
        lateral=lateral,
        attention=attention,
        fusion=fusion,
        quantization=quantization,
        reference=reference,
        upsample=upsample,
        manifest_index=index,
    )
