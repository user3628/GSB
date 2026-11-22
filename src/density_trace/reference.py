from __future__ import annotations

import hashlib
import itertools
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from .canonical import read_json, sha256_file
from .config import Config
from .errors import E_REFERENCE_INVALID, business_error
from .state import DensityState


@dataclass(frozen=True)
class QuantilePoint:
    index: int
    probability: float
    x_value: float
    y_value: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "probability": self.probability,
            "x_value": self.x_value,
            "y_value": self.y_value,
        }


@dataclass(frozen=True)
class ReferenceTable:
    path: Path
    schema_version: int
    name: str
    grid: dict[str, Any]
    curve: np.ndarray
    quantiles: tuple[QuantilePoint, ...]
    digest: str

    def quantile_map(self) -> dict[int, QuantilePoint]:
        return {point.index: point for point in self.quantiles}


def load_reference(path: str | Path, batch_id: int | None = None) -> ReferenceTable:
    reference_path = Path(path).resolve()
    try:
        raw = read_json(reference_path)
        table = ReferenceTable(
            path=reference_path,
            schema_version=int(raw.get("schema_version", 1)),
            name=str(raw.get("name", "reference")),
            grid=dict(raw["grid"]),
            curve=np.asarray(raw["curve"], dtype=np.float64),
            quantiles=tuple(
                QuantilePoint(
                    index=int(point["index"]),
                    probability=float(point["probability"]),
                    x_value=float(point["x_value"]),
                    y_value=float(point["y_value"]),
                )
                for point in raw["quantiles"]
            ),
            digest=sha256_file(reference_path),
        )
    except Exception as exc:
        raise business_error(
            E_REFERENCE_INVALID,
            f"failed to load reference quantile table: {exc}",
            batch_id=batch_id,
            quantile_index=None,
        ) from exc
    return table


def _reference_error(
    batch_id: int | None, quantile_index: int | None, message: str, **extra: Any
) -> None:
    raise business_error(
        E_REFERENCE_INVALID,
        message,
        batch_id=batch_id,
        quantile_index=quantile_index,
        **extra,
    )


def validate_reference(
    table: ReferenceTable, config: Config, batch_id: int | None = None
) -> None:
    if table.grid != config.grid.to_dict():
        _reference_error(
            batch_id,
            None,
            "reference grid does not match the active density grid",
            reference_grid=table.grid,
            active_grid=config.grid.to_dict(),
        )
    expected_grid_size = config.grid.nx * config.grid.ny
    if table.curve.ndim != 1 or table.curve.size != expected_grid_size:
        _reference_error(
            batch_id,
            None,
            "reference curve length does not match the query grid",
            expected_length=expected_grid_size,
            actual_length=int(table.curve.size),
        )
    if not np.all(np.isfinite(table.curve)) or np.any(table.curve < 0):
        bad = np.flatnonzero(~np.isfinite(table.curve) | (table.curve < 0))
        _reference_error(
            batch_id,
            None,
            "reference curve contains NaN, Inf, or negative values",
            grid_interval=[int(bad[0]), int(bad[-1])],
        )

    mapping = table.quantile_map()
    if len(mapping) != len(table.quantiles):
        duplicates = sorted(
            {
                point.index
                for point in table.quantiles
                if sum(item.index == point.index for item in table.quantiles) > 1
            }
        )
        index = duplicates[0] if duplicates else 0
        _reference_error(
            batch_id,
            index,
            "reference table contains duplicate quantile indices",
            duplicate_indices=duplicates,
        )
    if not table.quantiles:
        _reference_error(batch_id, 0, "reference table contains no quantiles")
    max_index = max(mapping)
    for index in range(max_index + 1):
        if index not in mapping:
            _reference_error(
                batch_id,
                index,
                "reference table is missing a quantile index",
                available_indices=sorted(mapping),
            )

    adjudication = config.adjudication
    for index in range(
        adjudication.quantile_index_start, adjudication.quantile_index_end + 1
    ):
        if index not in mapping:
            _reference_error(
                batch_id,
                index,
                "reference table is missing a quantile required for adjudication",
                quantile_index_start=adjudication.quantile_index_start,
                quantile_index_end=adjudication.quantile_index_end,
            )
    ordered = [mapping[index] for index in sorted(mapping)]
    if not all(np.isfinite(point.probability) for point in ordered):
        index = next(
            point.index for point in ordered if not np.isfinite(point.probability)
        )
        _reference_error(
            batch_id, index, "reference quantile probability is not finite"
        )
    if not all(
        np.isfinite(point.x_value) and np.isfinite(point.y_value) for point in ordered
    ):
        index = next(
            point.index
            for point in ordered
            if not np.isfinite(point.x_value) or not np.isfinite(point.y_value)
        )
        _reference_error(
            batch_id, index, "reference marginal quantile value is not finite"
        )
    for previous, current in itertools.pairwise(ordered):
        if current.probability <= previous.probability:
            _reference_error(
                batch_id,
                current.index,
                "reference quantile probabilities are not strictly increasing",
                previous_index=previous.index,
                previous_probability=previous.probability,
                current_probability=current.probability,
            )
        if current.x_value <= previous.x_value:
            _reference_error(
                batch_id,
                current.index,
                "reference x-marginal quantile values are not strictly increasing",
                previous_index=previous.index,
                previous_value=previous.x_value,
                current_value=current.x_value,
            )
        if current.y_value <= previous.y_value:
            _reference_error(
                batch_id,
                current.index,
                "reference y-marginal quantile values are not strictly increasing",
                previous_index=previous.index,
                previous_value=previous.y_value,
                current_value=current.y_value,
            )


def marginal_quantiles_from_fixed(
    density_units: np.ndarray,
    config: Config,
    probabilities: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    grid_size = config.grid.nx * config.grid.ny
    values = np.asarray(density_units, dtype=np.int64)
    if values.size != grid_size:
        raise business_error(
            E_REFERENCE_INVALID, "density curve size does not match the grid"
        )
    if np.any(values < 0):
        raise business_error(
            E_REFERENCE_INVALID, "density curve contains negative fixed-point values"
        )
    matrix = values.reshape(config.grid.ny, config.grid.nx)
    x_weights = matrix.sum(axis=0, dtype=np.longdouble)
    y_weights = matrix.sum(axis=1, dtype=np.longdouble)
    if np.sum(x_weights) <= 0 or np.sum(y_weights) <= 0:
        raise business_error(
            E_REFERENCE_INVALID, "density curve has no positive marginal mass"
        )
    xs = np.linspace(
        config.grid.x_min, config.grid.x_max, config.grid.nx, dtype=np.longdouble
    )
    ys = np.linspace(
        config.grid.y_min, config.grid.y_max, config.grid.ny, dtype=np.longdouble
    )
    return _weighted_axis_quantiles(
        xs, x_weights, probabilities
    ), _weighted_axis_quantiles(ys, y_weights, probabilities)


def _weighted_axis_quantiles(
    coordinates: np.ndarray,
    weights: np.ndarray,
    probabilities: np.ndarray,
) -> np.ndarray:
    total = np.sum(weights, dtype=np.longdouble)
    cumulative = np.cumsum(weights, dtype=np.longdouble)
    positions = (cumulative - 0.5 * weights) / total
    positions_float = positions.astype(np.float64)
    probabilities_long = np.asarray(probabilities, dtype=np.longdouble)
    result: list[np.longdouble] = []
    for probability in probabilities_long:
        if probability <= positions[0]:
            result.append(coordinates[0])
            continue
        if probability >= positions[-1]:
            result.append(coordinates[-1])
            continue
        high = int(np.searchsorted(positions_float, float(probability), side="left"))
        low = max(0, high - 1)
        span = positions[high] - positions[low]
        fraction = (probability - positions[low]) / span
        result.append(
            coordinates[low] + fraction * (coordinates[high] - coordinates[low])
        )
    return np.asarray(result, dtype=np.longdouble)


def _fixed_quantile_values(
    table: ReferenceTable,
    config: Config,
    batch_id: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    adjudication = config.adjudication
    indices = np.arange(
        adjudication.quantile_index_start,
        adjudication.quantile_index_end + 1,
        dtype=np.int64,
    )
    mapping = table.quantile_map()
    missing = [int(index) for index in indices if int(index) not in mapping]
    if missing:
        _reference_error(
            batch_id,
            missing[0],
            "reference table is missing a quantile required for adjudication",
        )
    points = [mapping[int(index)] for index in indices]
    probabilities = np.asarray(
        [point.probability for point in points], dtype=np.float64
    )
    x_values = np.asarray([point.x_value for point in points], dtype=np.float64)
    y_values = np.asarray([point.y_value for point in points], dtype=np.float64)
    scale = config.density.density_scale
    x_fixed = np.rint(x_values * float(scale)).astype(np.int64)
    y_fixed = np.rint(y_values * float(scale)).astype(np.int64)
    if not np.all(np.diff(x_fixed) > 0):
        bad_offset = int(np.flatnonzero(np.diff(x_fixed) <= 0)[0])
        _reference_error(
            batch_id,
            int(indices[bad_offset + 1]),
            "reference x-marginal quantiles collapse after fixed-point scaling",
        )
    if not np.all(np.diff(y_fixed) > 0):
        bad_offset = int(np.flatnonzero(np.diff(y_fixed) <= 0)[0])
        _reference_error(
            batch_id,
            int(indices[bad_offset + 1]),
            "reference y-marginal quantiles collapse after fixed-point scaling",
        )
    return indices, probabilities, x_fixed, y_fixed


def adjudicate_drift(
    state: DensityState,
    table: ReferenceTable,
    config: Config,
    batch_id: int,
) -> dict[str, Any]:
    validate_reference(table, config, batch_id)
    indices, probabilities, reference_x_fixed, reference_y_fixed = (
        _fixed_quantile_values(table, config, batch_id)
    )
    current_x, current_y = marginal_quantiles_from_fixed(
        state.density_units, config, probabilities
    )
    scale = float(state.scale)
    current_x_fixed = np.rint(current_x * scale).astype(np.int64)
    current_y_fixed = np.rint(current_y * scale).astype(np.int64)
    reference_x_span = np.longdouble(reference_x_fixed[-1] - reference_x_fixed[0])
    reference_y_span = np.longdouble(reference_y_fixed[-1] - reference_y_fixed[0])
    if reference_x_span <= 0:
        _reference_error(
            batch_id,
            int(indices[-1]),
            "reference x-marginal quantile span is not positive",
        )
    if reference_y_span <= 0:
        _reference_error(
            batch_id,
            int(indices[-1]),
            "reference y-marginal quantile span is not positive",
        )
    x_differences = (
        current_x_fixed.astype(np.longdouble) - reference_x_fixed.astype(np.longdouble)
    ) / reference_x_span
    y_differences = (
        current_y_fixed.astype(np.longdouble) - reference_y_fixed.astype(np.longdouble)
    ) / reference_y_span
    normalized_differences = np.concatenate((x_differences, y_differences))
    score = float(np.sqrt(np.mean(normalized_differences * normalized_differences)))
    adjudication = config.adjudication
    if score >= adjudication.drift_threshold:
        decision = "drift"
        threshold = adjudication.drift_threshold
        confidence = adjudication.drift_confidence
        is_drift = True
    elif score >= adjudication.warning_threshold:
        decision = "warning"
        threshold = adjudication.warning_threshold
        confidence = adjudication.warning_confidence
        is_drift = False
    else:
        decision = "stable"
        threshold = adjudication.warning_threshold
        confidence = adjudication.warning_confidence
        is_drift = False
    return {
        "batch_id": batch_id,
        "state_version": state.version,
        "state_version_fingerprint": state.version_fingerprint(
            config.config_fingerprint(table.digest)
        ),
        "score": score,
        "threshold": threshold,
        "thresholds": {
            "warning": adjudication.warning_threshold,
            "drift": adjudication.drift_threshold,
        },
        "confidence_levels": {
            "warning": adjudication.warning_confidence,
            "drift": adjudication.drift_confidence,
        },
        "decision": decision,
        "is_drift": is_drift,
        "confidence": confidence,
        "quantile_index_start": int(indices[0]),
        "quantile_index_end": int(indices[-1]),
        "quantile_count": int(indices.size),
        "quantile_probabilities": probabilities.tolist(),
        "current_x_quantiles_fixed": current_x_fixed.astype(int).tolist(),
        "current_y_quantiles_fixed": current_y_fixed.astype(int).tolist(),
        "reference_x_quantiles_fixed": reference_x_fixed.astype(int).tolist(),
        "reference_y_quantiles_fixed": reference_y_fixed.astype(int).tolist(),
        "score_scale": "x/y marginal reference quantile span in fixed-point domain",
        "decision_basis": (
            "rms over x/y marginals of (q_current-q_reference)/(q_reference[last]-q_reference[first]) "
            f"= {score:.17g}; warning={adjudication.warning_threshold:.17g}, "
            f"drift={adjudication.drift_threshold:.17g}"
        ),
        "reference_digest": table.digest,
        "reference_curve_sha256": "sha256:"
        + hashlib.sha256(table.curve.astype("<f8", copy=False).tobytes()).hexdigest(),
    }


def reference_quantile_profile(
    table: ReferenceTable,
    config: Config,
) -> tuple[np.ndarray, np.ndarray]:
    probabilities = np.asarray(
        [point.probability for point in table.quantiles], dtype=np.float64
    )
    fixed_curve = np.rint(table.curve * float(config.density.density_scale)).astype(
        np.int64
    )
    x_values, y_values = marginal_quantiles_from_fixed(
        fixed_curve, config, probabilities
    )
    return probabilities, np.column_stack((x_values, y_values))
