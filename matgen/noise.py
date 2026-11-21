"""Deterministic value-noise synthesis with octave-local error accounting hooks."""

from __future__ import annotations

import math
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal, localcontext
from typing import Any, Iterable

import numpy as np

from .canonical import fingerprint
from .errors import E_DIM_MISMATCH, E_NUMERIC, E_OCTAVE_RANGE, MatgenError


MASK64 = (1 << 64) - 1
_GOLDEN = 0x9E3779B97F4A7C15
_MIX_A = 0xBF58476D1CE4E5B9
_MIX_B = 0x94D049BB133111EB
_KEY_X = 0xD6E8FEB86659FD93
_KEY_Y = 0xA0761D6478BD642F
_TWO_POW_53 = 1 << 53
MAX_OCTAVES = 16
MAX_FREQUENCY = 4096.0
MAX_AMPLITUDE = 1_000_000.0
NOISE_SCHEMA = "matgen.noise.v1"


def _mix64_scalar(value: int) -> int:
    mixed = (value + _GOLDEN) & MASK64
    mixed = ((mixed ^ (mixed >> 30)) * _MIX_A) & MASK64
    mixed = ((mixed ^ (mixed >> 27)) * _MIX_B) & MASK64
    return (mixed ^ (mixed >> 31)) & MASK64


def _lattice_key(seed: int, octave_index: int, x_index: int, y_index: int) -> int:
    key = seed & MASK64
    key ^= ((octave_index + 1) * _GOLDEN) & MASK64
    key ^= (x_index * _KEY_X) & MASK64
    key ^= (y_index * _KEY_Y) & MASK64
    return key & MASK64


def _mix64_array(values: np.ndarray) -> np.ndarray:
    with np.errstate(over="ignore"):
        mixed = values.astype(np.uint64, copy=True)
        mixed += np.uint64(_GOLDEN)
        mixed = (mixed ^ (mixed >> np.uint64(30))) * np.uint64(_MIX_A)
        mixed = (mixed ^ (mixed >> np.uint64(27))) * np.uint64(_MIX_B)
        mixed = mixed ^ (mixed >> np.uint64(31))
    return mixed


def _lattice_keys(seed: int, octave_index: int, x_indices: np.ndarray, y_indices: np.ndarray) -> np.ndarray:
    with np.errstate(over="ignore"):
        keys = np.full(x_indices.shape, np.uint64(seed & MASK64), dtype=np.uint64)
        keys ^= np.uint64(((octave_index + 1) * _GOLDEN) & MASK64)
        keys ^= x_indices.astype(np.uint64) * np.uint64(_KEY_X)
        keys ^= y_indices.astype(np.uint64) * np.uint64(_KEY_Y)
    return keys


def _coordinate(index: int, size: int) -> float:
    return 0.0 if size <= 1 else float(index) / float(size - 1)


def _decimal_coordinate(index: int, size: int) -> Decimal:
    return Decimal(0) if size <= 1 else Decimal(index) / Decimal(size - 1)


def lattice_value_decimal(seed: int, octave_index: int, x_index: int, y_index: int) -> Decimal:
    key = _lattice_key(seed, octave_index, x_index, y_index)
    value_int = _mix64_scalar(key) >> 11
    return Decimal(value_int) / Decimal(_TWO_POW_53)


def value_noise_octave(
    seed: int,
    octave_index: int,
    frequency: float,
    height: int,
    width: int,
) -> np.ndarray:
    """Compute one smooth value-noise layer on a regular pixel grid."""

    x_coordinates = (
        np.zeros(width, dtype=np.float64)
        if width <= 1
        else np.arange(width, dtype=np.float64) / np.float64(width - 1)
    )
    y_coordinates = (
        np.zeros(height, dtype=np.float64)
        if height <= 1
        else np.arange(height, dtype=np.float64) / np.float64(height - 1)
    )
    u_values = x_coordinates * np.float64(frequency)
    v_values = y_coordinates * np.float64(frequency)

    x0 = np.floor(u_values).astype(np.int64)
    y0 = np.floor(v_values).astype(np.int64)
    tx = u_values - x0.astype(np.float64)
    ty = (v_values - y0.astype(np.float64))[:, None]
    sx = (tx * tx * (3.0 - 2.0 * tx))[None, :]
    sy = ty * ty * (3.0 - 2.0 * ty)

    x0_grid = np.broadcast_to(x0[None, :], (height, width))
    x1_grid = x0_grid + 1
    y0_grid = np.broadcast_to(y0[:, None], (height, width))
    y1_grid = y0_grid + 1

    v00 = _mix64_array(_lattice_keys(seed, octave_index, x0_grid, y0_grid)) >> np.uint64(11)
    v10 = _mix64_array(_lattice_keys(seed, octave_index, x1_grid, y0_grid)) >> np.uint64(11)
    v01 = _mix64_array(_lattice_keys(seed, octave_index, x0_grid, y1_grid)) >> np.uint64(11)
    v11 = _mix64_array(_lattice_keys(seed, octave_index, x1_grid, y1_grid)) >> np.uint64(11)

    scale = np.float64(1.0 / float(_TWO_POW_53))
    v00f = v00.astype(np.float64) * scale
    v10f = v10.astype(np.float64) * scale
    v01f = v01.astype(np.float64) * scale
    v11f = v11.astype(np.float64) * scale
    top = v00f + (v10f - v00f) * sx
    bottom = v01f + (v11f - v01f) * sx
    return top + (bottom - top) * sy


def decimal_value_noise_at_pixel(
    *,
    seed: int,
    octave_index: int,
    frequency: Decimal,
    x_index: int,
    y_index: int,
    width: int,
    height: int,
) -> Decimal:
    """High-precision scalar value-noise reference at one pixel center."""

    with localcontext() as context:
        context.prec = 60
        u_value = _decimal_coordinate(x_index, width) * frequency
        v_value = _decimal_coordinate(y_index, height) * frequency
        x0 = int(u_value.to_integral_value(rounding="ROUND_FLOOR"))
        y0 = int(v_value.to_integral_value(rounding="ROUND_FLOOR"))
        tx = u_value - Decimal(x0)
        ty = v_value - Decimal(y0)
        sx = tx * tx * (Decimal(3) - Decimal(2) * tx)
        sy = ty * ty * (Decimal(3) - Decimal(2) * ty)

        v00 = lattice_value_decimal(seed, octave_index, x0, y0)
        v10 = lattice_value_decimal(seed, octave_index, x0 + 1, y0)
        v01 = lattice_value_decimal(seed, octave_index, x0, y0 + 1)
        v11 = lattice_value_decimal(seed, octave_index, x0 + 1, y0 + 1)
        top = v00 + (v10 - v00) * sx
        bottom = v01 + (v11 - v01) * sx
        return top + (bottom - top) * sy


def make_resample_weights(target_size: int, source_size: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    if target_size <= 0 or source_size <= 0:
        raise ValueError("resample dimensions must be positive")
    if source_size == 1:
        base = np.zeros(target_size, dtype=np.int64)
        upper = np.zeros(target_size, dtype=np.int64)
        fraction = np.zeros(target_size, dtype=np.float64)
        return base, upper, fraction
    if target_size == 1:
        positions = np.zeros(1, dtype=np.float64)
    else:
        positions = (
            np.arange(target_size, dtype=np.float64)
            * np.float64(source_size - 1)
            / np.float64(target_size - 1)
        )
    base = np.floor(positions).astype(np.int64)
    base = np.clip(base, 0, source_size - 1)
    upper = np.minimum(base + 1, source_size - 1)
    fraction = positions - base.astype(np.float64)
    return base, upper, fraction


def bilinear_resample(field: np.ndarray, height: int, width: int) -> np.ndarray:
    source_height, source_width = field.shape
    y0, y1, fy = make_resample_weights(height, source_height)
    x0, x1, fx = make_resample_weights(width, source_width)
    top = field[y0[:, None], x0[None, :]] * (1.0 - fx[None, :]) + field[y0[:, None], x1[None, :]] * fx[None, :]
    bottom = field[y1[:, None], x0[None, :]] * (1.0 - fx[None, :]) + field[y1[:, None], x1[None, :]] * fx[None, :]
    return top * (1.0 - fy[:, None]) + bottom * fy[:, None]


def decimal_resample_gray(
    *,
    seed: int,
    octave_definitions: Iterable[dict[str, Any]],
    normalized_amplitudes: Iterable[Decimal],
    source_width: int,
    source_height: int,
    target_x: int,
    target_y: int,
    target_width: int,
    target_height: int,
) -> tuple[Decimal, float]:
    """Reference the final resampled grayscale value at one target pixel."""

    with localcontext() as context:
        context.prec = 70
        source_x = (
            Decimal(0)
            if target_width <= 1
            else Decimal(target_x) * Decimal(source_width - 1) / Decimal(target_width - 1)
        )
        source_y = (
            Decimal(0)
            if target_height <= 1
            else Decimal(target_y) * Decimal(source_height - 1) / Decimal(target_height - 1)
        )
        x0 = int(source_x.to_integral_value(rounding="ROUND_FLOOR"))
        y0 = int(source_y.to_integral_value(rounding="ROUND_FLOOR"))
        x1 = min(x0 + 1, source_width - 1)
        y1 = min(y0 + 1, source_height - 1)
        fx = source_x - Decimal(x0)
        fy = source_y - Decimal(y0)

        total = Decimal(0)
        for octave_entry, amplitude in zip(octave_definitions, normalized_amplitudes):
            values: list[Decimal] = []
            for base_y in (y0, y1):
                for base_x in (x0, x1):
                    values.append(
                        decimal_value_noise_at_pixel(
                            seed=seed,
                            octave_index=octave_entry["index"],
                            frequency=octave_entry["frequency_decimal"],
                            x_index=base_x,
                            y_index=base_y,
                            width=source_width,
                            height=source_height,
                        )
                    )
            top = values[0] * (Decimal(1) - fx) + values[1] * fx
            bottom = values[2] * (Decimal(1) - fx) + values[3] * fx
            octave_value = top * (Decimal(1) - fy) + bottom * fy
            total += amplitude * octave_value
        return total, float(fx), float(fy)


def validate_target_resolution(recipe: dict[str, Any]) -> tuple[int, int]:
    material_id = recipe.get("material_id")
    resolution = recipe.get("resolution")
    if isinstance(resolution, dict):
        width = resolution.get("width")
        height = resolution.get("height")
    elif isinstance(resolution, (list, tuple)) and len(resolution) == 2:
        width, height = resolution
    else:
        raise MatgenError(
            code=E_DIM_MISMATCH,
            message="resolution must be [width, height]",
            material_id=material_id,
            stage="palette",
        )
    if (
        isinstance(width, bool)
        or isinstance(height, bool)
        or not isinstance(width, int)
        or not isinstance(height, int)
        or not 1 <= width <= 4096
        or not 1 <= height <= 4096
    ):
        raise MatgenError(
            code=E_DIM_MISMATCH,
            message="target resolution must contain integers in [1, 4096]",
            material_id=material_id,
            stage="palette",
            details={"resolution": resolution},
        )
    return width, height


def resolve_base_resolution(
    recipe: dict[str, Any], config: dict[str, Any], target_width: int, target_height: int
) -> tuple[int, int]:
    material_id = recipe.get("material_id")
    noise_config = recipe.get("noise")
    if not isinstance(noise_config, dict):
        raise MatgenError(
            code=E_NUMERIC,
            message="noise configuration must be an object",
            material_id=material_id,
            stage="noise",
        )
    declared = noise_config.get("base_resolution")
    if declared is None:
        base_width = max(config["noise_base_min"], target_width * config["noise_base_scale"])
        base_height = max(config["noise_base_min"], target_height * config["noise_base_scale"])
        base_width = min(config["noise_base_max"], base_width)
        base_height = min(config["noise_base_max"], base_height)
        return base_width, base_height
    if isinstance(declared, dict):
        width = declared.get("width")
        height = declared.get("height")
    elif isinstance(declared, (list, tuple)) and len(declared) == 2:
        width, height = declared
    else:
        width = height = None
    if (
        isinstance(width, bool)
        or isinstance(height, bool)
        or not isinstance(width, int)
        or not isinstance(height, int)
        or not 1 <= width <= 4096
        or not 1 <= height <= 4096
    ):
        raise MatgenError(
            code=E_DIM_MISMATCH,
            message="noise.base_resolution must contain integers in [1, 4096]",
            material_id=material_id,
            stage="noise",
            details={"base_resolution": declared},
        )
    return width, height


def validate_noise_definition(recipe: dict[str, Any]) -> tuple[int, list[dict[str, Any]], list[float], list[Decimal]]:
    """Validate and canonicalize all octave and amplitude inputs."""

    material_id = recipe.get("material_id")
    noise_config = recipe.get("noise")
    if not isinstance(noise_config, dict):
        raise MatgenError(
            code=E_NUMERIC,
            message="noise configuration must be an object",
            material_id=material_id,
            stage="noise",
        )
    seed = noise_config.get("seed")
    if isinstance(seed, bool) or not isinstance(seed, int) or not 0 <= seed <= MASK64:
        raise MatgenError(
            code=E_NUMERIC,
            message="noise seed must be an unsigned 64-bit integer",
            material_id=material_id,
            stage="noise",
            details={"seed": repr(seed)},
        )
    octaves = noise_config.get("octaves")
    if not isinstance(octaves, list) or not 1 <= len(octaves) <= MAX_OCTAVES:
        raise MatgenError(
            code=E_OCTAVE_RANGE,
            message=f"octaves must contain between 1 and {MAX_OCTAVES} entries",
            material_id=material_id,
            stage="noise",
            details={"octave_count": len(octaves) if isinstance(octaves, list) else None},
        )

    definitions: list[dict[str, Any]] = []
    amplitudes: list[float] = []
    decimal_amplitudes: list[Decimal] = []
    for index, entry in enumerate(octaves):
        if not isinstance(entry, dict):
            raise MatgenError(
                code=E_OCTAVE_RANGE,
                message="each octave must be an object",
                material_id=material_id,
                stage="noise",
                octave_index=index,
            )
        frequency = entry.get("frequency")
        amplitude = entry.get("amplitude")
        if (
            isinstance(frequency, bool)
            or isinstance(amplitude, bool)
            or not isinstance(frequency, (int, float))
            or not isinstance(amplitude, (int, float))
        ):
            raise MatgenError(
                code=E_NUMERIC,
                message="octave frequency and amplitude must be numeric",
                material_id=material_id,
                stage="noise",
                octave_index=index,
                details={"frequency": repr(frequency), "amplitude": repr(amplitude)},
            )
        frequency_float = float(frequency)
        amplitude_float = float(amplitude)
        if not math.isfinite(frequency_float) or not math.isfinite(amplitude_float):
            raise MatgenError(
                code=E_NUMERIC,
                message="non-finite octave parameter",
                material_id=material_id,
                stage="noise",
                octave_index=index,
                details={"frequency": repr(frequency), "amplitude": repr(amplitude)},
            )
        if not 0.0 < frequency_float <= MAX_FREQUENCY:
            raise MatgenError(
                code=E_OCTAVE_RANGE,
                message="octave frequency is outside the supported range",
                material_id=material_id,
                stage="noise",
                octave_index=index,
                details={"frequency": frequency_float, "maximum": MAX_FREQUENCY},
            )
        if not 0.0 < amplitude_float <= MAX_AMPLITUDE:
            raise MatgenError(
                code=E_OCTAVE_RANGE,
                message="octave amplitude is outside the supported range",
                material_id=material_id,
                stage="noise",
                octave_index=index,
                details={"amplitude": amplitude_float, "maximum": MAX_AMPLITUDE},
            )
        frequency_decimal = Decimal(str(frequency)) if isinstance(frequency, float) else Decimal(frequency)
        amplitude_decimal = Decimal(str(amplitude)) if isinstance(amplitude, float) else Decimal(amplitude)
        definitions.append(
            {
                "index": index,
                "frequency": frequency_float,
                "amplitude": amplitude_float,
                "frequency_decimal": frequency_decimal,
                "amplitude_decimal": amplitude_decimal,
            }
        )
        amplitudes.append(amplitude_float)
        decimal_amplitudes.append(amplitude_decimal)
    return seed, definitions, amplitudes, decimal_amplitudes


def compose_noise(
    recipe: dict[str, Any],
    config: dict[str, Any],
    *,
    max_threads: int | None = None,
) -> dict[str, Any]:
    """Synthesize all octave layers and reduce them in a fixed order."""

    material_id = recipe.get("material_id")
    target_width, target_height = validate_target_resolution(recipe)
    base_width, base_height = resolve_base_resolution(recipe, config, target_width, target_height)
    seed, definitions, amplitudes, decimal_amplitudes = validate_noise_definition(recipe)
    amplitude_sum = math.fsum(amplitudes)
    normalized_amplitudes = [amplitude / amplitude_sum for amplitude in amplitudes]
    decimal_amplitude_sum = sum(decimal_amplitudes, Decimal(0))
    decimal_normalized_amplitudes = [amplitude / decimal_amplitude_sum for amplitude in decimal_amplitudes]
    workers = config["max_threads"] if max_threads is None else max_threads
    if not isinstance(workers, int) or not 1 <= workers <= 64:
        raise MatgenError(
            code=E_NUMERIC,
            message="max_threads must be an integer in [1, 64]",
            material_id=material_id,
            stage="noise",
        )

    def compute_octave(index: int) -> np.ndarray:
        entry = definitions[index]
        return value_noise_octave(
            seed=seed,
            octave_index=index,
            frequency=entry["frequency"],
            height=base_height,
            width=base_width,
        )

    octave_fields: list[np.ndarray | None] = [None] * len(definitions)
    if workers == 1 or len(definitions) == 1:
        for index in range(len(definitions)):
            octave_fields[index] = compute_octave(index)
    else:
        with ThreadPoolExecutor(max_workers=min(workers, len(definitions))) as executor:
            futures = [executor.submit(compute_octave, index) for index in range(len(definitions))]
            for index, future in enumerate(futures):
                octave_fields[index] = future.result()

    composed = np.zeros((base_height, base_width), dtype=np.float64)
    concrete_octaves: list[np.ndarray] = []
    for index, octave_field in enumerate(octave_fields):
        if octave_field is None:
            raise AssertionError("octave worker returned no field")
        concrete_octaves.append(octave_field)
        composed = composed + octave_field * np.float64(normalized_amplitudes[index])

    if not np.all(np.isfinite(composed)):
        bad_flat = int(np.flatnonzero(~np.isfinite(composed))[0])
        raise MatgenError(
            code=E_NUMERIC,
            message="composed noise contains non-finite values",
            material_id=material_id,
            stage="noise",
            details={"flat_index": bad_flat},
        )

    return {
        "schema": NOISE_SCHEMA,
        "material_id": material_id,
        "seed": seed,
        "octave_count": len(definitions),
        "octave_definitions": definitions,
        "amplitudes": amplitudes,
        "normalized_amplitudes": normalized_amplitudes,
        "decimal_normalized_amplitudes": decimal_normalized_amplitudes,
        "base_width": base_width,
        "base_height": base_height,
        "target_width": target_width,
        "target_height": target_height,
        "field": composed,
        "octave_fields": concrete_octaves,
        "field_fingerprint": fingerprint(
            {
                "shape": [base_height, base_width],
                "values": [float(value) for value in composed.ravel()],
            }
        ),
    }


def recipe_noise_fingerprint(recipe: dict[str, Any], config: dict[str, Any], control_digest: str) -> str:
    target_width, target_height = validate_target_resolution(recipe)
    base_width, base_height = resolve_base_resolution(recipe, config, target_width, target_height)
    noise_config = recipe.get("noise", {})
    return fingerprint(
        {
            "noise": noise_config,
            "target_resolution": [target_width, target_height],
            "base_resolution": [base_width, base_height],
            "control_digest": control_digest,
        }
    )

