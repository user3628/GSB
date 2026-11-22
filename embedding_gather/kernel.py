from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Mapping

import numpy as np

from .batches import IndexBatch
from .digest import canonical_json_bytes, sha256_json
from .errors import (
    E_BATCH_INVALID,
    E_DIM_MISMATCH,
    E_EMPTY_BATCH,
    E_INDEX_NONINTEGER,
    E_INDEX_OOB,
    E_INVALID_STAGE,
    E_NUMERIC,
    E_WEIGHT_SHAPE,
    fail,
)
from .weights import WeightTable


@dataclass(frozen=True)
class GatherConfig:
    output_scale: float | None = None
    negative_index_policy: str = "wrap"
    verify_atol_scale: float = 0.0

    def __post_init__(self) -> None:
        if self.negative_index_policy not in {"wrap", "reject"}:
            raise fail(
                E_BATCH_INVALID,
                "negative_index_policy must be wrap or reject",
                negative_index_policy=self.negative_index_policy,
            )
        if self.output_scale is not None and (not math.isfinite(self.output_scale) or self.output_scale <= 0):
            raise fail(
                E_NUMERIC,
                "configured output_scale must be finite and positive",
                output_scale=self.output_scale,
            )
        if self.verify_atol_scale < 0:
            raise fail(E_BATCH_INVALID, "verify_atol_scale must be non-negative")

    def fingerprint_components(self) -> dict[str, Any]:
        return {
            "negative_index_policy": self.negative_index_policy,
            "output_scale": None if self.output_scale is None else float(self.output_scale).hex(),
            "verify_atol_scale": float(self.verify_atol_scale).hex(),
        }


@dataclass(frozen=True)
class BatchValidation:
    batch: IndexBatch
    gather_axis: int
    ids: np.ndarray
    weights: np.ndarray | None
    shard_ids: tuple[str, ...]
    mapped_ids: np.ndarray
    output_shape: tuple[int, ...]

    def to_metadata(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch.batch_id,
            "batch_number": self.batch.batch_number,
            "ids_shape": list(self.ids.shape),
            "ids_dtype": self.ids.dtype.str,
            "weights_present": self.weights is not None,
            "weights_digest": self.batch.weights_digest,
            "gather_axis": self.gather_axis,
            "output_shape": list(self.output_shape),
            "shard_ids": list(self.shard_ids),
            "ids_digest": self.batch.ids_digest,
            "metadata_digest": self.batch.metadata_digest,
        }


@dataclass(frozen=True)
class GatherComputation:
    validation: BatchValidation
    quantization_bits: int
    shard_quantization: Mapping[str, Mapping[str, Any]]
    units: np.ndarray
    output_scale: float
    reference: np.ndarray
    error_bound: np.ndarray
    nonpad_counts: np.ndarray
    pad_counts: np.ndarray
    weight_abs_sum: np.ndarray
    hits: tuple[dict[str, Any], ...]

    @property
    def pad_hit_count(self) -> int:
        return int(np.sum(self.pad_counts, dtype=np.int64))


@dataclass(frozen=True)
class QuantizedResult:
    computation: GatherComputation
    output_q: np.ndarray
    output_scale: float
    error_bound: np.ndarray
    observed_error: np.ndarray
    hits: tuple[dict[str, Any], ...]
    hits_json: bytes
    metadata: dict[str, Any]

    @property
    def pad_hit_count(self) -> int:
        return self.computation.pad_hit_count

    def arrays(self) -> dict[str, np.ndarray]:
        return {
            "output_q": np.asarray(self.output_q, dtype=np.int64),
            "output_scale": np.asarray(self.output_scale, dtype=np.float64),
            "error_bound": np.asarray(self.error_bound, dtype=np.float64),
            "observed_error": np.asarray(self.observed_error, dtype=np.float64),
            "pad_hit_count": np.asarray(self.pad_hit_count, dtype=np.int64),
            "hits_json": np.frombuffer(self.hits_json, dtype=np.uint8),
            "metadata_json": np.frombuffer(canonical_json_bytes(self.metadata), dtype=np.uint8),
        }


def normalize_gather_axis(axis: int | None, ndim: int, *, batch_id: str) -> int:
    if ndim == 0:
        if axis not in (None, 0, -1):
            raise fail(
                E_BATCH_INVALID,
                "scalar index arrays only accept gather_axis 0, -1, or None",
                batch_id=batch_id,
                gather_axis=axis,
            )
        return 0
    candidate = ndim - 1 if axis is None else int(axis)
    if candidate < 0:
        candidate += ndim
    if candidate < 0 or candidate >= ndim:
        raise fail(
            E_BATCH_INVALID,
            "gather_axis is outside the index array rank",
            batch_id=batch_id,
            gather_axis=axis,
            ids_ndim=ndim,
        )
    return candidate


def _output_shape(shape: tuple[int, ...], axis: int, dim: int) -> tuple[int, ...]:
    if len(shape) == 0:
        return (dim,)
    return tuple(item for index, item in enumerate(shape) if index != axis) + (dim,)


def _output_key(flat_index: int, shape: tuple[int, ...], axis: int) -> tuple[int, ...]:
    if len(shape) == 0:
        return ()
    indices = np.unravel_index(flat_index, shape)
    return tuple(int(value) for index, value in enumerate(indices) if index != axis)


def _ids_index(flat_index: int, shape: tuple[int, ...]) -> tuple[int, ...]:
    if len(shape) == 0:
        return ()
    return tuple(int(value) for value in np.unravel_index(flat_index, shape))


def _integer_dtype(dtype: np.dtype[Any]) -> bool:
    return np.issubdtype(dtype, np.integer) and not np.issubdtype(dtype, np.bool_)


def validate_batch(table: WeightTable, batch: IndexBatch, config: GatherConfig) -> BatchValidation:
    ids = np.asarray(batch.ids)
    if ids.size == 0:
        raise fail(
            E_EMPTY_BATCH,
            "index batch is empty",
            batch_id=batch.batch_id,
            shard_id=None,
            index_id=None,
            ids_shape=list(ids.shape),
        )
    if not _integer_dtype(ids.dtype):
        bad = next(
            (tuple(int(v) for v in index), float(value))
            for index, value in np.ndenumerate(ids)
            if isinstance(value, (float, np.floating)) and float(value).is_integer()
        ) if np.issubdtype(ids.dtype, np.floating) else None
        raise fail(
            E_INDEX_NONINTEGER,
            "index ids must use an integer dtype",
            batch_id=batch.batch_id,
            ids_dtype=ids.dtype.str,
            first_fractional=_first_fractional(ids),
            integral_float_example=None if bad is None else {"index": list(bad[0]), "value": bad[1]},
        )
    weights = batch.weights
    if weights is not None:
        weights = np.asarray(weights)
        if weights.shape != ids.shape:
            raise fail(
                E_WEIGHT_SHAPE,
                "per-element weights must have exactly the index array shape",
                batch_id=batch.batch_id,
                ids_shape=list(ids.shape),
                weights_shape=list(weights.shape),
            )
        if not np.issubdtype(weights.dtype, np.number) or np.issubdtype(weights.dtype, np.complexfloating):
            raise fail(
                E_NUMERIC,
                "gather weights must be real numeric values",
                batch_id=batch.batch_id,
                weights_dtype=weights.dtype.str,
            )

    axis = normalize_gather_axis(batch.gather_axis, ids.ndim, batch_id=batch.batch_id)
    mapped = np.empty(ids.shape, dtype=np.int64)
    shard_ids: set[str] = set()
    total_rows = table.project.total_rows
    for index, raw in np.ndenumerate(ids):
        value = int(raw)
        if value < 0:
            if config.negative_index_policy == "reject":
                raise fail(
                    E_INDEX_OOB,
                    "negative index rejected by policy",
                    batch_id=batch.batch_id,
                    shard_id=None,
                    index_id=value,
                    ids_index=list(index),
                    negative_index_policy=config.negative_index_policy,
                )
            value = value + total_rows
            if value < 0:
                raise fail(
                    E_INDEX_OOB,
                    "negative index is still outside the table after wrapping",
                    batch_id=batch.batch_id,
                    shard_id=None,
                    index_id=int(raw),
                    mapped_id=value,
                    total_rows=total_rows,
                )
        shard = table.project.find_shard(value)
        if shard is None:
            raise fail(
                E_INDEX_OOB,
                "index id is outside every shard row interval",
                batch_id=batch.batch_id,
                shard_id=None,
                index_id=int(raw),
                mapped_id=value,
                total_rows=total_rows,
            )
        runtime = table.shards[shard.shard_id]
        local_row = value - shard.row_offset
        if local_row < 0 or local_row >= runtime.weights_q.shape[0]:
            raise fail(
                E_INDEX_OOB,
                "index id maps outside the weight array using the shard row offset",
                batch_id=batch.batch_id,
                shard_id=shard.shard_id,
                index_id=int(raw),
                mapped_id=value,
                row_offset=shard.row_offset,
                local_row=local_row,
                weight_rows=int(runtime.weights_q.shape[0]),
            )
        shard_ids.add(shard.shard_id)
        mapped[index] = value
    return BatchValidation(
        batch=batch,
        gather_axis=axis,
        ids=ids,
        weights=weights,
        shard_ids=tuple(sorted(shard_ids)),
        mapped_ids=mapped,
        output_shape=_output_shape(ids.shape, axis, table.project.embedding_dim),
    )


def _first_fractional(ids: np.ndarray) -> dict[str, Any] | None:
    if not np.issubdtype(ids.dtype, np.floating):
        return None
    for index, value in np.ndenumerate(ids):
        if not math.isfinite(float(value)) or not float(value).is_integer():
            return {"index": list(index), "value": float(value)}
    return None


def _resolve_output_scale(table: WeightTable, config: GatherConfig) -> float:
    if config.output_scale is not None:
        return float(config.output_scale)
    scales = [runtime.scale for runtime in table.shards.values() if math.isfinite(runtime.scale) and runtime.scale > 0]
    if not scales:
        return float(next(iter(table.shards.values())).scale)
    return min(scales)


def gather(
    table: WeightTable,
    batch: IndexBatch,
    config: GatherConfig | None = None,
    validation: BatchValidation | None = None,
) -> GatherComputation:
    config = config or GatherConfig()
    validation = validation or validate_batch(table, batch, config)
    dim = table.project.embedding_dim
    output_scale = _resolve_output_scale(table, config)
    shape = validation.ids.shape
    axis = validation.gather_axis
    out_shape = validation.output_shape
    unit_accum: dict[tuple[int, ...], int] = {key: 0 for key in np.ndindex(out_shape)}
    reference_accum: dict[tuple[int, ...], np.longdouble] = {
        key: np.longdouble(0) for key in np.ndindex(out_shape)
    }
    error_accum: dict[tuple[int, ...], np.longdouble] = {
        key: np.longdouble(0) for key in np.ndindex(out_shape)
    }
    nonpad_counts: dict[tuple[int, ...], int] = {key: 0 for key in np.ndindex(out_shape[:-1])}
    pad_counts: dict[tuple[int, ...], int] = {key: 0 for key in np.ndindex(out_shape[:-1])}
    weight_abs_sum: dict[tuple[int, ...], np.longdouble] = {
        key: np.longdouble(0) for key in np.ndindex(out_shape[:-1])
    }
    quantization_error_sum: dict[tuple[int, ...], np.longdouble] = {
        key: np.longdouble(0) for key in np.ndindex(out_shape[:-1])
    }
    first_context: dict[tuple[int, ...], tuple[str, int, int]] = {}
    hits: list[dict[str, Any]] = []
    flat_ids = np.asarray(validation.ids).reshape(-1)
    flat_mapped = np.asarray(validation.mapped_ids).reshape(-1)
    flat_weights = None if validation.weights is None else np.asarray(validation.weights).reshape(-1)

    for flat_index, (original, mapped) in enumerate(zip(flat_ids, flat_mapped, strict=True)):
        indices = _ids_index(flat_index, shape)
        output_key = _output_key(flat_index, shape, axis)
        shard = table.project.find_shard(int(mapped))
        if shard is None:
            raise fail(
                E_INDEX_OOB,
                "resolved index no longer maps to a shard",
                batch_id=batch.batch_id,
                shard_id=None,
                index_id=int(original),
                mapped_id=int(mapped),
            )
        runtime = table.shards[shard.shard_id]
        local_row = int(mapped) - shard.row_offset
        weight_value = 1.0 if flat_weights is None else float(flat_weights[flat_index])
        if not math.isfinite(weight_value):
            raise fail(
                E_NUMERIC,
                "gather weight is NaN or infinity",
                batch_id=batch.batch_id,
                shard_id=shard.shard_id,
                index_id=int(original),
                mapped_id=int(mapped),
                ids_index=list(indices),
                component="weight",
                weight=weight_value,
            )
        if not math.isfinite(runtime.scale) or runtime.scale <= 0.0:
            raise fail(
                E_NUMERIC,
                "weight shard scale is not finite and positive",
                batch_id=batch.batch_id,
                shard_id=shard.shard_id,
                index_id=int(original),
                mapped_id=int(mapped),
                component="scale",
                scale=runtime.scale,
            )
        if not math.isfinite(output_scale) or output_scale <= 0.0:
            raise fail(
                E_NUMERIC,
                "output scale is not finite and positive",
                batch_id=batch.batch_id,
                shard_id=shard.shard_id,
                index_id=int(original),
                component="output_scale",
                output_scale=output_scale,
            )
        is_pad = shard.is_pad_row(int(mapped))
        hit = {
            "occurrence": flat_index,
            "ids_index": list(indices),
            "output_index": list(output_key),
            "shard_id": shard.shard_id,
            "index_id": int(original),
            "mapped_id": int(mapped),
            "local_row": local_row,
            "weight": weight_value,
            "is_pad": is_pad,
        }
        hits.append(hit)
        if is_pad:
            pad_counts[output_key] += 1
            continue
        rows = runtime.weights_q[local_row]
        if rows.ndim != 1 or rows.shape[0] != dim:
            raise fail(
                E_DIM_MISMATCH,
                "resolved embedding row has the wrong dimension",
                batch_id=batch.batch_id,
                shard_id=shard.shard_id,
                index_id=int(original),
                local_row=local_row,
                actual_dim=int(rows.shape[0]) if rows.ndim else 0,
                expected_dim=dim,
            )
        if np.issubdtype(rows.dtype, np.floating) and not np.all(np.isfinite(rows)):
            component = int(np.flatnonzero(~np.isfinite(rows))[0])
            raise fail(
                E_NUMERIC,
                "embedding row contains NaN or infinity",
                batch_id=batch.batch_id,
                shard_id=shard.shard_id,
                index_id=int(original),
                mapped_id=int(mapped),
                component=component,
                value=float(rows[component]),
            )
        if np.issubdtype(rows.dtype, np.floating):
            fractional = np.flatnonzero(rows != np.floor(rows))
            if fractional.size:
                component = int(fractional[0])
                raise fail(
                    E_NUMERIC,
                    "embedding weights are not integral fixed-point values",
                    batch_id=batch.batch_id,
                    shard_id=shard.shard_id,
                    index_id=int(original),
                    mapped_id=int(mapped),
                    component=component,
                    value=float(rows[component]),
                )
        qmin = -(1 << (table.project.quantization_bits - 1))
        qmax = (1 << (table.project.quantization_bits - 1)) - 1
        below = np.flatnonzero(np.asarray(rows) < qmin)
        above = np.flatnonzero(np.asarray(rows) > qmax)
        component_outside = int(below[0]) if below.size else (int(above[0]) if above.size else None)
        if component_outside is not None:
            raise fail(
                E_NUMERIC,
                "embedding weight exceeds the declared quantization bit width",
                batch_id=batch.batch_id,
                shard_id=shard.shard_id,
                index_id=int(original),
                mapped_id=int(mapped),
                component=component_outside,
                value=int(rows[component_outside]),
                quantization_bits=table.project.quantization_bits,
            )
        ratio = np.longdouble(runtime.scale) / np.longdouble(output_scale)
        if not np.isfinite(ratio) or ratio <= 0:
            raise fail(
                E_NUMERIC,
                "scale ratio is invalid",
                batch_id=batch.batch_id,
                shard_id=shard.shard_id,
                index_id=int(original),
                component="scale_ratio",
                shard_scale=runtime.scale,
                output_scale=output_scale,
            )
        if output_key not in first_context:
            first_context[output_key] = (shard.shard_id, int(original), local_row)
        nonpad_counts[output_key] += 1
        weight_abs_sum[output_key] += np.longdouble(abs(weight_value))
        quantization_error_sum[output_key] += (
            np.longdouble(abs(weight_value)) * np.longdouble(runtime.scale) * np.longdouble(0.5)
        )
        contribution_abs = np.longdouble(0)
        for component in range(dim):
            component_key = output_key + (component,)
            q_value = int(rows[component])
            exact = np.longdouble(q_value) * np.longdouble(runtime.scale) * np.longdouble(weight_value)
            units_long = np.rint(exact / np.longdouble(output_scale))
            if not np.isfinite(units_long):
                raise fail(
                    E_NUMERIC,
                    "fixed-point conversion produced a non-finite value",
                    batch_id=batch.batch_id,
                    shard_id=shard.shard_id,
                    index_id=int(original),
                    component=component,
                )
            contribution_units = int(units_long)
            if contribution_units < -(1 << 63) or contribution_units >= (1 << 63):
                raise fail(
                    E_NUMERIC,
                    "fixed-point contribution overflows int64",
                    batch_id=batch.batch_id,
                    shard_id=shard.shard_id,
                    index_id=int(original),
                    component=component,
                    contribution_units=contribution_units,
                )
            base = unit_accum[component_key] + contribution_units
            if base < -(1 << 63) or base >= (1 << 63):
                raise fail(
                    E_NUMERIC,
                    "fixed-point aggregate overflows int64",
                    batch_id=batch.batch_id,
                    shard_id=shard.shard_id,
                    index_id=int(original),
                    component=component,
                    aggregate_units=base,
                )
            unit_accum[component_key] = base
            reference_accum[component_key] += exact
            error_accum[component_key] += abs(exact) * np.finfo(np.longdouble).eps * np.longdouble(8)
            contribution_abs += abs(exact)

    units = np.zeros(out_shape, dtype=np.int64)
    reference = np.zeros(out_shape, dtype=np.float64)
    error_bound = np.zeros(out_shape, dtype=np.float64)
    nonpad_array = np.zeros(out_shape[:-1], dtype=np.int64)
    pad_array = np.zeros(out_shape[:-1], dtype=np.int64)
    weight_abs_array = np.zeros(out_shape[:-1], dtype=np.float64)
    for cell in np.ndindex(out_shape[:-1]):
        if nonpad_counts[cell] == 0 and pad_counts[cell] == 0:
            raise fail(
                E_INVALID_STAGE,
                "gather produced an output cell without any contributing index",
                batch_id=batch.batch_id,
                output_index=list(cell),
            )
        if nonpad_counts[cell] > 0 and weight_abs_sum[cell] == 0:
            shard_id, index_id, _ = first_context[cell]
            raise fail(
                E_NUMERIC,
                "all aggregation weights for this output cell are zero",
                batch_id=batch.batch_id,
                shard_id=shard_id,
                index_id=index_id,
                output_index=list(cell),
                component="*",
            )
        nonpad_array[cell] = nonpad_counts[cell]
        pad_array[cell] = pad_counts[cell]
        weight_abs_array[cell] = np.float64(weight_abs_sum[cell])
        count = nonpad_counts[cell]
        rounding_bound = np.longdouble(count) * np.longdouble(output_scale) * np.longdouble(0.5)
        for component in range(dim):
            key = cell + (component,)
            units[key] = unit_accum[key]
            reference_value = reference_accum[key]
            if not np.isfinite(reference_value) or abs(reference_value) > np.longdouble(np.finfo(np.float64).max):
                shard_id, index_id, _ = first_context[cell]
                raise fail(
                    E_NUMERIC,
                    "high-precision reference is outside the serializable range",
                    batch_id=batch.batch_id,
                    shard_id=shard_id,
                    index_id=index_id,
                    component=component,
                    output_index=list(cell),
                )
            reference[key] = np.float64(reference_value)
            calculated = rounding_bound + error_accum[key] + quantization_error_sum[cell]
            observed = abs(
                np.longdouble(unit_accum[key]) * np.longdouble(output_scale) - reference_accum[key]
            )
            conservative = max(calculated, observed) * (np.longdouble(1) + np.longdouble(32) * np.finfo(np.longdouble).eps)
            conservative += np.longdouble(output_scale) * np.finfo(np.float64).eps
            bound_value = np.nextafter(np.float64(conservative), np.float64(np.inf))
            if not np.isfinite(bound_value):
                shard_id, index_id, _ = first_context[cell]
                raise fail(
                    E_NUMERIC,
                    "error bound is not finite",
                    batch_id=batch.batch_id,
                    shard_id=shard_id,
                    index_id=index_id,
                    component=component,
                    output_index=list(cell),
                )
            error_bound[key] = bound_value
    return GatherComputation(
        validation=validation,
        quantization_bits=table.project.quantization_bits,
        shard_quantization={
            shard_id: {
                "scale": float(table.shards[shard_id].scale).hex(),
                "max_abs_rounding_error": float(table.shards[shard_id].scale) * 0.5,
                "storage_bits": int(table.shards[shard_id].quantization_bits),
            }
            for shard_id in validation.shard_ids
        },
        units=units,
        output_scale=output_scale,
        reference=reference,
        error_bound=error_bound,
        nonpad_counts=nonpad_array,
        pad_counts=pad_array,
        weight_abs_sum=weight_abs_array,
        hits=tuple(hits),
    )


def quantize(computation: GatherComputation, config: GatherConfig | None = None) -> QuantizedResult:
    config = config or GatherConfig()
    output_scale = computation.output_scale
    reference_long = np.asarray(computation.reference, dtype=np.longdouble)
    fixed_long = np.asarray(computation.units, dtype=np.longdouble) * np.longdouble(output_scale)
    observed = np.abs(fixed_long - reference_long)
    allowed = np.asarray(computation.error_bound, dtype=np.longdouble)
    tolerance = np.longdouble(config.verify_atol_scale)
    violations = np.argwhere(observed > allowed + tolerance)
    if violations.size:
        position = tuple(int(item) for item in violations[0])
        shard_id = computation.hits[0]["shard_id"] if computation.hits else None
        index_id = computation.hits[0]["index_id"] if computation.hits else None
        raise fail(
            E_NUMERIC,
            "observed fixed-point error exceeds the declared error bound",
            batch_id=computation.validation.batch.batch_id,
            shard_id=shard_id,
            index_id=index_id,
            output_index=list(position),
            component="quantization_error",
            observed_error=float(observed[position]),
            error_bound=float(allowed[position]),
        )
    hits_json = canonical_json_bytes(list(computation.hits))
    metadata = {
        "batch_id": computation.validation.batch.batch_id,
        "batch_number": computation.validation.batch.batch_number,
        "table_id": computation.validation.batch.table_id,
        "gather_axis": computation.validation.gather_axis,
        "ids_shape": list(computation.validation.ids.shape),
        "output_shape": list(computation.validation.output_shape),
        "output_dtype": "int64_fixed_point",
        "output_scale": float(output_scale).hex(),
        "quantization_bits": int(computation.quantization_bits),
        "shard_quantization": dict(computation.shard_quantization),
        "shard_ids": list(computation.validation.shard_ids),
        "pad_hit_count": computation.pad_hit_count,
        "hit_count": len(computation.hits),
        "ids_digest": computation.validation.batch.ids_digest,
        "weights_digest": computation.validation.batch.weights_digest,
        "config_digest": sha256_json(config.fingerprint_components()),
        "error_model": "sum(abs(weight)*shard_scale/2) + nonpad_count*output_scale/2 + longdouble_slack",
    }
    return QuantizedResult(
        computation=computation,
        output_q=np.asarray(computation.units, dtype=np.int64),
        output_scale=output_scale,
        error_bound=np.asarray(computation.error_bound, dtype=np.float64),
        observed_error=np.asarray(observed, dtype=np.float64),
        hits=computation.hits,
        hits_json=hits_json,
        metadata=metadata,
    )
