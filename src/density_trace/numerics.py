from __future__ import annotations

import math
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import numpy as np

from .canonical import canonical_json_bytes, sha256_bytes
from .config import Config
from .errors import E_NUMERIC, business_error
from .manifest import LoadedBatch


@dataclass(frozen=True)
class BatchDensity:
    batch_id: int
    effective_samples: int
    bandwidth: tuple[float, float]
    density_units: np.ndarray
    blocks: tuple[dict[str, Any], ...]
    error_report: dict[str, Any]
    local_curve_sha256: str

    def density_curve(self, scale: int) -> np.ndarray:
        return self.density_units.astype(np.float64) / float(scale)


def _compensated_add(
    values: np.ndarray,
    contributions: np.ndarray,
    compensation: np.ndarray,
    strategy: str,
) -> None:
    if strategy == "kahan":
        adjusted = contributions - compensation
        updated = values + adjusted
        compensation[...] = (updated - values) - adjusted
        values[...] = updated
        return
    updated = values + contributions
    use_values = np.abs(values) >= np.abs(contributions)
    compensation[use_values] += (
        values[use_values] - updated[use_values]
    ) + contributions[use_values]
    compensation[~use_values] += (
        contributions[~use_values] - updated[~use_values]
    ) + values[~use_values]
    values[...] = updated


def _raise_numeric(
    batch_id: int,
    block_index: int,
    grid_interval: list[int],
    anomaly: str,
    **extra: Any,
) -> None:
    raise business_error(
        E_NUMERIC,
        "non-finite value encountered in scaled density accumulation",
        batch_id=batch_id,
        block_index=block_index,
        grid_interval=grid_interval,
        anomaly=anomaly,
        **extra,
    )


def robust_bandwidth(samples: np.ndarray, config: Config, batch_id: int) -> np.ndarray:
    rule = config.density.bandwidth
    median = np.median(samples, axis=0)
    mad = np.median(np.abs(samples - median), axis=0) * rule.mad_factor
    q25, q75 = np.percentile(samples, [25.0, 75.0], axis=0)
    iqr_scale = (q75 - q25) / rule.iqr_factor
    bandwidth = np.where(
        mad > rule.floor, mad, np.where(iqr_scale > rule.floor, iqr_scale, rule.floor)
    )
    if not np.all(np.isfinite(bandwidth)) or np.any(bandwidth <= 0):
        raise business_error(
            E_NUMERIC,
            "bandwidth estimate is not finite and positive",
            batch_id=batch_id,
            anomaly="invalid bandwidth",
            grid_interval=[0, int(config.grid.nx * config.grid.ny) - 1],
            bandwidth=bandwidth.tolist(),
        )
    return bandwidth.astype(np.float64, copy=False)


def _contribution_vector(
    sample: np.ndarray,
    grid: np.ndarray,
    bandwidth: np.ndarray,
    coefficient: float,
) -> np.ndarray:
    delta_x = (grid[:, 0] - sample[0]) / bandwidth[0]
    delta_y = (grid[:, 1] - sample[1]) / bandwidth[1]
    log_kernel = -0.5 * (delta_x * delta_x + delta_y * delta_y)
    return np.exp(log_kernel) * coefficient


def evaluate_batch_density(
    batch: LoadedBatch,
    config: Config,
    batch_id: int | None = None,
) -> BatchDensity:
    active_batch_id = batch.spec.batch_id if batch_id is None else batch_id
    scale = config.density.density_scale
    grid = config.grid.points()
    grid_size = int(grid.shape[0])
    if (
        isinstance(scale, bool)
        or not isinstance(scale, (int, np.integer))
        or scale <= 0
    ):
        _raise_numeric(
            active_batch_id,
            0,
            [0, grid_size - 1],
            f"density_scale={scale!r}",
            scale=scale,
        )
    if not np.isfinite(float(scale)):
        _raise_numeric(
            active_batch_id,
            0,
            [0, grid_size - 1],
            f"density_scale={scale!r}",
            scale=scale,
        )
    samples = batch.effective_samples
    if samples.shape[0] == 0:
        raise business_error(
            "E_EMPTY_BATCH",
            "batch has no effective samples",
            batch_id=active_batch_id,
        )
    if not np.all(np.isfinite(samples)):
        bad_rows, bad_columns = np.where(~np.isfinite(samples))
        _raise_numeric(
            active_batch_id,
            0,
            [0, grid_size - 1],
            f"sample row={int(bad_rows[0])} column={int(bad_columns[0])} value={samples[bad_rows[0], bad_columns[0]]!r}",
        )
    if not np.all(np.isfinite(grid)):
        _raise_numeric(
            active_batch_id, 0, [0, grid_size - 1], "query grid contains NaN or Inf"
        )

    bandwidth = robust_bandwidth(samples, config, active_batch_id)
    normalization = 1.0 / (2.0 * math.pi * bandwidth[0] * bandwidth[1])
    coefficient = normalization / float(samples.shape[0]) * float(scale)
    if not np.isfinite(coefficient) or coefficient == 0.0:
        _raise_numeric(
            active_batch_id,
            0,
            [0, grid_size - 1],
            f"scaled kernel coefficient={coefficient!r}",
            bandwidth=bandwidth.tolist(),
            scale=scale,
        )

    chunk_size = config.density.chunk_size
    strategy = config.density.sum_strategy
    blocks: list[dict[str, Any]] = []
    total_units = np.zeros(grid_size, dtype=np.int64)
    total_abs_scaled = 0.0
    high_precision_reference = np.zeros(grid_size, dtype=np.longdouble)
    coefficient_high = np.longdouble(
        normalization / float(samples.shape[0]) * float(scale)
    )

    for block_index, start in enumerate(range(0, samples.shape[0], chunk_size)):
        end = min(start + chunk_size, samples.shape[0])
        values = np.zeros(grid_size, dtype=np.float64)
        compensation = np.zeros(grid_size, dtype=np.float64)
        abs_scaled = 0.0
        for sample_index in range(start, end):
            sample = samples[sample_index]
            contributions = _contribution_vector(sample, grid, bandwidth, coefficient)
            if not np.all(np.isfinite(contributions)):
                bad = np.flatnonzero(~np.isfinite(contributions))
                first = int(bad[0])
                last = int(bad[-1])
                _raise_numeric(
                    active_batch_id,
                    block_index,
                    [first, last],
                    f"block contribution contains NaN or Inf at grid index {first}",
                )
            abs_scaled += float(np.sum(np.abs(contributions), dtype=np.float64))
            _compensated_add(values, contributions, compensation, strategy)

            delta_x = (grid[:, 0] - sample[0]) / bandwidth[0]
            delta_y = (grid[:, 1] - sample[1]) / bandwidth[1]
            log_kernel = -0.5 * (delta_x * delta_x + delta_y * delta_y)
            high_precision_reference += (
                np.exp(log_kernel.astype(np.longdouble)) * coefficient_high
            )

        compensated = values + compensation
        if not np.all(np.isfinite(compensated)):
            bad = np.flatnonzero(~np.isfinite(compensated))
            _raise_numeric(
                active_batch_id,
                block_index,
                [int(bad[0]), int(bad[-1])],
                "compensated partial sum contains NaN or Inf",
            )
        if np.max(np.abs(compensated)) > float(np.iinfo(np.int64).max):
            _raise_numeric(
                active_batch_id,
                block_index,
                [0, grid_size - 1],
                "scaled partial sum exceeds int64 range",
            )
        block_units = np.rint(compensated).astype(np.int64)
        total_units += block_units
        total_abs_scaled += abs_scaled
        blocks.append(
            {
                "block_index": block_index,
                "sample_start": start,
                "sample_end": end,
                "partial_sum_units": block_units.tolist(),
                "partial_sum_sha256": sha256_bytes(block_units.astype("<i8").tobytes()),
            }
        )

    if np.finfo(np.longdouble).eps < np.finfo(np.float64).eps:
        reference_density_units = np.rint(high_precision_reference).astype(
            np.longdouble
        )
        precision_bits = int(np.finfo(np.longdouble).nmant + 1)
        high_precision_method = f"numpy.longdouble ({precision_bits}-bit significand)"
    else:
        decimal_reference = [Decimal(0)] * grid_size
        for sample in samples:
            contributions = _contribution_vector(sample, grid, bandwidth, coefficient)
            for grid_index, value in enumerate(contributions.tolist()):
                decimal_reference[grid_index] += Decimal(value)
        reference_density_units = np.asarray(
            [float(value) for value in decimal_reference], dtype=np.longdouble
        )
        high_precision_method = (
            "decimal.Decimal exact accumulation of float64 contributions"
        )

    eps = float(np.finfo(np.float64).eps)
    block_count = len(blocks)
    quantization_bound_units = 0.5 * block_count
    floating_bound_units = 16.0 * eps * total_abs_scaled
    error_bound_units = quantization_bound_units + floating_bound_units + 1.0
    observed_error_units = float(
        np.max(np.abs(total_units.astype(np.longdouble) - reference_density_units))
    )
    max_local_density = float(np.max(total_units.astype(np.float64) / float(scale)))
    error_bound_density = error_bound_units / float(scale)
    relative_bound = error_bound_density / max(
        max_local_density, np.finfo(np.float64).tiny
    )
    significant_digits = max(
        0, math.floor(-math.log10(max(relative_bound, np.finfo(np.float64).tiny)))
    )
    local_curve = total_units.astype("<i8").tobytes()
    error_report = {
        "batch_id": active_batch_id,
        "effective_samples": int(samples.shape[0]),
        "scale": int(scale),
        "sum_strategy": strategy,
        "chunk_size": chunk_size,
        "block_count": block_count,
        "block_order": [[item["sample_start"], item["sample_end"]] for item in blocks],
        "bound_formula": "0.5*n_blocks + 16*eps*sum_abs_scaled + safety_units",
        "quantization_bound_units": quantization_bound_units,
        "floating_bound_units": floating_bound_units,
        "safety_units": 1.0,
        "error_bound_units": error_bound_units,
        "error_bound_density": error_bound_density,
        "observed_error_units": observed_error_units,
        "observed_error_density": observed_error_units / float(scale),
        "within_bound": observed_error_units <= error_bound_units,
        "significant_digits": significant_digits,
        "high_precision_method": high_precision_method,
        "max_local_density": max_local_density,
    }
    if not error_report["within_bound"]:
        raise business_error(
            E_NUMERIC,
            "observed accumulation error exceeds the explicit error bound",
            batch_id=active_batch_id,
            grid_interval=[0, grid_size - 1],
            anomaly="error bound violated",
            error_report=error_report,
        )
    return BatchDensity(
        batch_id=active_batch_id,
        effective_samples=int(samples.shape[0]),
        bandwidth=(float(bandwidth[0]), float(bandwidth[1])),
        density_units=total_units,
        blocks=tuple(blocks),
        error_report=error_report,
        local_curve_sha256=sha256_bytes(local_curve),
    )


def fixed_curve_units(curve: np.ndarray, scale: int) -> np.ndarray:
    values = np.asarray(curve, dtype=np.float64)
    if not np.all(np.isfinite(values)):
        raise business_error(
            E_NUMERIC, "curve contains NaN or Inf", anomaly="non-finite curve"
        )
    return np.rint(values * float(scale)).astype(np.int64)


def curve_bytes(density_units: np.ndarray, total_samples: int, scale: int) -> bytes:
    payload = (
        density_units.astype("<i8", copy=False).tobytes()
        + int(total_samples).to_bytes(8, "little", signed=True)
        + int(scale).to_bytes(8, "little", signed=True)
    )
    return payload


def curve_digest(density_units: np.ndarray, total_samples: int, scale: int) -> str:
    return sha256_bytes(curve_bytes(density_units, total_samples, scale))


def state_payload_digest(density_units: np.ndarray, grid_weights: np.ndarray) -> str:
    return sha256_bytes(
        density_units.astype("<i8", copy=False).tobytes()
        + grid_weights.astype("<i8", copy=False).tobytes()
    )


def weighted_quantiles_fixed(
    values: np.ndarray, probabilities: np.ndarray
) -> np.ndarray:
    fixed = np.asarray(values, dtype=np.int64)
    if fixed.ndim != 1 or fixed.size == 0:
        raise business_error(
            E_NUMERIC,
            "quantile input must be a nonempty vector",
            anomaly="empty quantile vector",
        )
    order = np.argsort(fixed, kind="stable")
    sorted_values = fixed[order].astype(np.longdouble)
    weights = np.ones(fixed.size, dtype=np.longdouble)
    cumulative = np.cumsum(weights)
    positions = (cumulative - 0.5 * weights) / cumulative[-1]
    probabilities_long = np.asarray(probabilities, dtype=np.longdouble)
    positions_float = positions.astype(np.float64)
    result: list[np.longdouble] = []
    for probability in probabilities_long:
        if probability <= positions[0]:
            result.append(sorted_values[0])
            continue
        if probability >= positions[-1]:
            result.append(sorted_values[-1])
            continue
        high = int(np.searchsorted(positions_float, float(probability), side="left"))
        low = max(0, high - 1)
        span = positions[high] - positions[low]
        fraction = (probability - positions[low]) / span
        result.append(
            sorted_values[low] + fraction * (sorted_values[high] - sorted_values[low])
        )
    return np.asarray(result, dtype=np.longdouble)


def digest_record_for_versions(value: Any) -> str:
    return sha256_bytes(canonical_json_bytes(value))
