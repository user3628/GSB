"""Local weight/activation loading with deterministic fingerprints."""

from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path

import numpy as np

from .canonical import array_digest, canonical_digest
from .errors import E_INVALID_INPUT, E_QUANT_INVALID, AuditError


@dataclass(frozen=True)
class LayerWeights:
    name: str
    weight: np.ndarray
    bias: np.ndarray

    @property
    def fingerprint(self) -> str:
        return canonical_digest(
            {
                "weight": array_digest(self.weight),
                "bias": array_digest(self.bias),
            }
        )


@dataclass(frozen=True)
class WeightBundle:
    path: str
    layers: tuple[LayerWeights, ...]

    @property
    def layer_names(self) -> tuple[str, ...]:
        return tuple(layer.name for layer in self.layers)

    @property
    def fingerprint(self) -> str:
        return canonical_digest({layer.name: layer.fingerprint for layer in self.layers})


@dataclass(frozen=True)
class CalibrationBundle:
    path: str
    source_shape: tuple[int, ...]
    samples: np.ndarray

    @property
    def fingerprint(self) -> str:
        return canonical_digest(
            {
                "source_shape": list(self.source_shape),
                "selected": array_digest(self.samples),
            }
        )


def load_weights(path: str | Path) -> WeightBundle:
    source = Path(path)
    if not source.exists():
        raise AuditError(E_INVALID_INPUT, "weight shard does not exist", path=str(source))
    try:
        with np.load(source, allow_pickle=False) as archive:
            arrays = {key: np.asarray(archive[key]) for key in archive.files}
    except Exception as exc:
        raise AuditError(E_INVALID_INPUT, f"cannot read weight NPZ: {exc}", path=str(source)) from exc
    names = _extract_layer_names(arrays)
    layers: list[LayerWeights] = []
    for index, name in enumerate(names):
        weight = _find_array(arrays, name, index, "weight")
        bias = _find_array(arrays, name, index, "bias")
        weight = _as_float_matrix(weight, layer=name, parameter="weight")
        bias = _as_bias(bias, layer=name)
        if weight.shape[0] != bias.shape[0]:
            raise AuditError(
                E_INVALID_INPUT,
                "weight/bias shape mismatch",
                layer=name,
                weight_shape=list(weight.shape),
                bias_shape=list(bias.shape),
            )
        if not np.all(np.isfinite(weight)) or not np.all(np.isfinite(bias)):
            raise AuditError(E_INVALID_INPUT, "weight shard contains NaN or Inf", layer=name)
        layers.append(LayerWeights(name=name, weight=weight, bias=bias))
    _validate_chain(layers)
    return WeightBundle(path=str(source), layers=tuple(layers))


def load_calibration(path: str | Path, ratio: float) -> CalibrationBundle:
    source = Path(path)
    if not source.exists():
        raise AuditError(E_INVALID_INPUT, "calibration tensor does not exist", path=str(source))
    try:
        samples = np.load(source, allow_pickle=False)
    except Exception as exc:
        raise AuditError(E_INVALID_INPUT, f"cannot read calibration NPY: {exc}", path=str(source)) from exc
    samples = np.asarray(samples)
    if samples.ndim != 2 or samples.shape[0] == 0 or samples.shape[1] == 0:
        raise AuditError(E_INVALID_INPUT, "calibration tensor must be a non-empty 2-D array", shape=list(samples.shape))
    if not np.issubdtype(samples.dtype, np.number):
        raise AuditError(E_INVALID_INPUT, "calibration tensor must be numeric", dtype=str(samples.dtype))
    samples = np.ascontiguousarray(samples.astype(np.float64, copy=False))
    subset_count = max(1, math.ceil(samples.shape[0] * ratio))
    subset = np.ascontiguousarray(samples[:subset_count])
    return CalibrationBundle(path=str(source), source_shape=tuple(samples.shape), samples=subset)


def ensure_calibration_finite(samples: np.ndarray, layer_name: str) -> None:
    if not np.all(np.isfinite(samples)):
        raise AuditError(
            E_QUANT_INVALID,
            "activation tensor contains NaN or Inf",
            layer=layer_name,
        )


def _extract_layer_names(arrays: dict[str, np.ndarray]) -> list[str]:
    for key in ("layer_names", "layer_order", "layers"):
        if key in arrays:
            raw = arrays[key]
            if raw.ndim == 0:
                value = raw.item()
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                if isinstance(value, str) and "," in value:
                    names = [part.strip() for part in value.split(",") if part.strip()]
                else:
                    names = [str(value)]
            else:
                names = []
                for item in raw.tolist():
                    if isinstance(item, bytes):
                        item = item.decode("utf-8")
                    names.append(str(item))
            if names:
                if len(set(names)) != len(names):
                    raise AuditError(E_INVALID_INPUT, "duplicate layer names in manifest", layers=names)
                return names
    inferred = sorted(
        key.rsplit(".", 1)[0]
        for key in arrays
        if key.endswith(".weight")
    )
    if not inferred:
        raise AuditError(E_INVALID_INPUT, "weight shard has no layer manifest or '<layer>.weight' arrays")
    return inferred


def _find_array(arrays: dict[str, np.ndarray], name: str, index: int, kind: str) -> np.ndarray:
    candidates = [
        f"{name}.{kind}",
        f"{name}/{kind}",
        f"{name}_{kind}",
        f"{kind}_{index}",
        f"{'W' if kind == 'weight' else 'b'}{index}",
        f"{'weights' if kind == 'weight' else 'biases'}_{index}",
    ]
    for candidate in candidates:
        if candidate in arrays:
            return arrays[candidate]
    raise AuditError(E_INVALID_INPUT, f"missing {kind} array", layer=name, expected_keys=candidates)


def _as_float_matrix(value: np.ndarray, layer: str, parameter: str) -> np.ndarray:
    if value.ndim != 2:
        raise AuditError(E_INVALID_INPUT, f"{parameter} must be 2-D", layer=layer, shape=list(value.shape))
    if not np.issubdtype(value.dtype, np.number):
        raise AuditError(E_INVALID_INPUT, f"{parameter} must be numeric", layer=layer, dtype=str(value.dtype))
    return np.ascontiguousarray(value.astype(np.float64, copy=False))


def _as_bias(value: np.ndarray, layer: str) -> np.ndarray:
    if value.ndim == 2 and value.shape[0] == 1:
        value = value[0]
    if value.ndim != 1:
        raise AuditError(E_INVALID_INPUT, "bias must be 1-D or shape (1, out)", layer=layer, shape=list(value.shape))
    if not np.issubdtype(value.dtype, np.number):
        raise AuditError(E_INVALID_INPUT, "bias must be numeric", layer=layer, dtype=str(value.dtype))
    return np.ascontiguousarray(value.astype(np.float64, copy=False))


def _validate_chain(layers: list[LayerWeights]) -> None:
    if not layers:
        raise AuditError(E_INVALID_INPUT, "weight shard contains no layers")
    for previous, current in pairwise(layers):
        if previous.weight.shape[0] != current.weight.shape[1]:
            raise AuditError(
                E_INVALID_INPUT,
                "adjacent layers have incompatible dimensions",
                previous_layer=previous.name,
                previous_output=previous.weight.shape[0],
                next_layer=current.name,
                next_input=current.weight.shape[1],
            )

