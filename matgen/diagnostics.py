"""Cross-octave floating-point error tracking and budget diagnostics."""

from __future__ import annotations

import math
from decimal import Decimal, localcontext
from typing import Any

import numpy as np

from .canonical import fingerprint
from .errors import E_NUMERIC, MatgenError
from .noise import _mix64_scalar, decimal_resample_gray, decimal_value_noise_at_pixel
from .numeric import assert_finite_report_value, significant_digits
from .palette import (
    BASE_TINT_SCALE,
    METALNESS_CONTROL_SCALE,
    METALNESS_ENERGY_SCALE,
    MODULATION_BIAS_SCALE,
    MODULATION_ENERGY_SCALE,
    MODULATION_X_SCALE,
    ROUGHNESS_CONTROL_SCALE,
    ROUGHNESS_ENERGY_SCALE,
    SHADE_OFFSET,
    SHADE_SCALE,
    nearest_palette_indices,
)


DIAGNOSTICS_SCHEMA = "matgen.diagnostics.v1"
EPSILON = np.finfo(np.float64).eps


def _decimal_clip(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    return min(max(value, low), high)


def _decimal_text(value: Decimal) -> str:
    return str(value)


def _check_bound(value: float, *, material_id: str, octave_index: int | None, label: str) -> float:
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0.0:
        raise MatgenError(
            code=E_NUMERIC,
            message=f"invalid diagnostic error bound: {label}",
            material_id=material_id,
            stage="diagnostics",
            octave_index=octave_index,
            details={"label": label, "value": repr(value)},
        )
    return numeric


def deterministic_sample_positions(
    *,
    width: int,
    height: int,
    sample_count: int,
    sample_seed: int,
    material_id: str,
    purpose: str,
) -> list[dict[str, Any]]:
    """Select distinct pixel positions with a reproducible SplitMix64 stream."""

    total_pixels = width * height
    count = min(sample_count, total_pixels)
    seed_material = fingerprint(
        {
            "sample_seed": sample_seed,
            "material_id": material_id,
            "width": width,
            "height": height,
            "purpose": purpose,
        }
    )
    seed64 = int(seed_material[:16], 16)
    selected: list[dict[str, Any]] = []
    seen: set[tuple[int, int]] = set()
    attempt = 0
    while len(selected) < count:
        mix_x = _mix64_scalar(seed64 ^ ((attempt + 1) * 0xD6E8FEB86659FD93))
        mix_y = _mix64_scalar(seed64 ^ ((attempt + 1) * 0xA0761D6478BD642F) ^ 0xE7037ED1A0B428DB)
        x_index = int(mix_x % width)
        y_index = int(mix_y % height)
        attempt += 1
        if (x_index, y_index) in seen:
            if attempt > total_pixels * 64 + 1024:
                raise MatgenError(
                    code=E_NUMERIC,
                    message="unable to select distinct deterministic sample positions",
                    material_id=material_id,
                    stage="diagnostics",
                )
            continue
        seen.add((x_index, y_index))
        normalized_x = Decimal(0) if width <= 1 else Decimal(x_index) / Decimal(width - 1)
        normalized_y = Decimal(0) if height <= 1 else Decimal(y_index) / Decimal(height - 1)
        selected.append(
            {
                "x": x_index,
                "y": y_index,
                "normalized_x": _decimal_text(normalized_x),
                "normalized_y": _decimal_text(normalized_y),
            }
        )
    return selected


def _octave_error_report(
    *,
    recipe: dict[str, Any],
    config: dict[str, Any],
    control_artifact: dict[str, Any],
    noise_artifact: dict[str, Any],
    base_positions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], float]:
    material_id = str(recipe["material_id"])
    seed = int(noise_artifact["seed"])
    base_width = int(noise_artifact["base_width"])
    base_height = int(noise_artifact["base_height"])
    octave_definitions = [
        {
            **definition,
            "frequency_decimal": Decimal(str(definition["frequency_decimal"])),
        }
        for definition in noise_artifact["octave_definitions"]
    ]
    float_amplitudes = [float(value) for value in noise_artifact["normalized_amplitudes"]]
    decimal_amplitudes = [
        Decimal(str(value)) for value in noise_artifact["decimal_normalized_amplitudes"]
    ]
    x_indices = np.asarray([position["x"] for position in base_positions], dtype=np.int64)
    y_indices = np.asarray([position["y"] for position in base_positions], dtype=np.int64)
    sample_total = len(base_positions)

    float_accumulator = np.zeros(sample_total, dtype=np.float64)
    decimal_accumulator = [Decimal(0) for _ in range(sample_total)]
    accumulated_bounds = np.zeros(sample_total, dtype=np.float64)
    previous_bounds = np.zeros(sample_total, dtype=np.float64)
    reports: list[dict[str, Any]] = []
    octave_budget = float(config["octave_error_budget"])

    for octave_index, (definition, amplitude_float, amplitude_decimal) in enumerate(
        zip(octave_definitions, float_amplitudes, decimal_amplitudes)
    ):
        actual_octave = np.asarray(noise_artifact["octave_fields"][octave_index], dtype=np.float64)
        actual_values = actual_octave[y_indices, x_indices]
        term_float = actual_values * np.float64(amplitude_float)
        float_accumulator = float_accumulator + term_float
        if not np.all(np.isfinite(float_accumulator)):
            raise MatgenError(
                code=E_NUMERIC,
                message="non-finite float accumulation during diagnostics",
                material_id=material_id,
                stage="diagnostics",
                octave_index=octave_index,
            )

        with localcontext() as context:
            context.prec = 70
            reference_octave_values = [
                decimal_value_noise_at_pixel(
                    seed=seed,
                    octave_index=octave_index,
                    frequency=definition["frequency_decimal"],
                    x_index=int(position["x"]),
                    y_index=int(position["y"]),
                    width=base_width,
                    height=base_height,
                )
                for position in base_positions
            ]
            reference_terms = [
                amplitude_decimal * reference_value for reference_value in reference_octave_values
            ]
            for sample_index in range(sample_total):
                decimal_accumulator[sample_index] += reference_terms[sample_index]

        measured_term_errors = np.asarray(
            [
                abs(float(term_float[sample_index]) - float(reference_terms[sample_index]))
                for sample_index in range(sample_total)
            ],
            dtype=np.float64,
        )
        rounding_bounds = np.asarray(
            [
                EPSILON
                * (
                    abs(float(float_accumulator[sample_index]))
                    + abs(float(term_float[sample_index]))
                    + 1.0
                )
                * 8.0
                for sample_index in range(sample_total)
            ],
            dtype=np.float64,
        )
        structural_bounds = accumulated_bounds + measured_term_errors + rounding_bounds
        measured_accumulation_errors = np.asarray(
            [
                abs(float(float_accumulator[sample_index]) - float(decimal_accumulator[sample_index]))
                for sample_index in range(sample_total)
            ],
            dtype=np.float64,
        )
        accumulated_bounds = np.maximum(
            structural_bounds,
            measured_accumulation_errors + 8.0 * EPSILON,
        )
        if not np.all(np.isfinite(accumulated_bounds)):
            raise MatgenError(
                code=E_NUMERIC,
                message="non-finite octave error accumulation",
                material_id=material_id,
                stage="diagnostics",
                octave_index=octave_index,
            )
        incremental_bounds = accumulated_bounds - previous_bounds
        previous_bounds = accumulated_bounds.copy()
        sample_reports = []
        for sample_index, position in enumerate(base_positions):
            reference_value = decimal_accumulator[sample_index]
            float_value = float(float_accumulator[sample_index])
            sample_error = measured_accumulation_errors[sample_index]
            sample_bound = accumulated_bounds[sample_index]
            assert_finite_report_value(
                float_value,
                material_id=material_id,
                stage="diagnostics",
                octave_index=octave_index,
                label="float_accumulated_octave_value",
            )
            assert_finite_report_value(
                float(reference_value),
                material_id=material_id,
                stage="diagnostics",
                octave_index=octave_index,
                label="decimal_reference_value",
            )
            sample_reports.append(
                {
                    "position": position,
                    "float_value": float_value,
                    "reference_decimal": _decimal_text(reference_value),
                    "absolute_error": _check_bound(
                        float(sample_error),
                        material_id=material_id,
                        octave_index=octave_index,
                        label="sample_absolute_error",
                    ),
                    "error_bound": _check_bound(
                        float(sample_bound),
                        material_id=material_id,
                        octave_index=octave_index,
                        label="sample_error_bound",
                    ),
                }
            )

        representative_reference = decimal_accumulator[0]
        bound_max = float(np.max(accumulated_bounds))
        relative_bound = bound_max / max(abs(float(representative_reference)), 1e-300)
        if not math.isfinite(relative_bound):
            raise MatgenError(
                code=E_NUMERIC,
                message="non-finite relative error bound",
                material_id=material_id,
                stage="diagnostics",
                octave_index=octave_index,
            )
        contribution_bound = float(np.max(incremental_bounds))
        absolute_contribution = max(
            sum((abs(value) for value in reference_terms), Decimal(0)) / Decimal(sample_total),
            Decimal("1e-300"),
        )
        occupancy = contribution_bound / octave_budget
        reports.append(
            {
                "octave_index": octave_index,
                "frequency": definition["frequency"],
                "amplitude_raw": definition["amplitude"],
                "amplitude_normalized": amplitude_float,
                "log_contribution": math.log10(float(absolute_contribution)),
                "log_amplitude_contribution": math.log2(amplitude_float),
                "log_contribution_basis": "log10(mean absolute Decimal octave contribution over recorded samples)",
                "contribution_bound": _check_bound(
                    contribution_bound,
                    material_id=material_id,
                    octave_index=octave_index,
                    label="contribution_bound",
                ),
                "accumulation_error_bound": _check_bound(
                    bound_max,
                    material_id=material_id,
                    octave_index=octave_index,
                    label="accumulation_error_bound",
                ),
                "measured_absolute_error": _check_bound(
                    float(np.max(measured_accumulation_errors)),
                    material_id=material_id,
                    octave_index=octave_index,
                    label="measured_absolute_error",
                ),
                "relative_error_bound": relative_bound,
                "effective_digits": significant_digits(float(representative_reference), bound_max),
                "error_source": "lattice-value exactness + smoothstep interpolation + float64 ordered accumulation",
                "octave_budget": octave_budget,
                "budget_occupancy": occupancy,
                "over_budget": occupancy > 1.0,
                "budget_margin": octave_budget - contribution_bound,
                "samples": sample_reports,
            }
        )
    return reports, float(np.max(accumulated_bounds)) if reports else 0.0


def _reference_target_samples(
    *,
    recipe: dict[str, Any],
    noise_artifact: dict[str, Any],
    palette_artifact: dict[str, Any],
    target_positions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    material_id = str(recipe["material_id"])
    source_width = int(noise_artifact["base_width"])
    source_height = int(noise_artifact["base_height"])
    target_width = int(noise_artifact["target_width"])
    target_height = int(noise_artifact["target_height"])
    octave_definitions = [
        {
            **definition,
            "frequency_decimal": Decimal(str(definition["frequency_decimal"])),
        }
        for definition in noise_artifact["octave_definitions"]
    ]
    decimal_amplitudes = [
        Decimal(str(value)) for value in noise_artifact["decimal_normalized_amplitudes"]
    ]
    actual_gray = palette_artifact["gray_resampled"]
    references: list[dict[str, Any]] = []
    for position in target_positions:
        reference_gray, fraction_x, fraction_y = decimal_resample_gray(
            seed=int(noise_artifact["seed"]),
            octave_definitions=octave_definitions,
            normalized_amplitudes=decimal_amplitudes,
            source_width=source_width,
            source_height=source_height,
            target_x=int(position["x"]),
            target_y=int(position["y"]),
            target_width=target_width,
            target_height=target_height,
        )
        actual_value = float(actual_gray[int(position["y"]), int(position["x"])])
        assert_finite_report_value(
            actual_value,
            material_id=material_id,
            stage="diagnostics",
            label="resampled_gray",
        )
        assert_finite_report_value(
            float(reference_gray),
            material_id=material_id,
            stage="diagnostics",
            label="decimal_resampled_gray",
        )
        references.append(
            {
                "position": position,
                "float_value": actual_value,
                "reference_decimal": _decimal_text(reference_gray),
                "absolute_error": abs(actual_value - float(reference_gray)),
                "resample_fraction_x": fraction_x,
                "resample_fraction_y": fraction_y,
            }
        )
    return references


def _palette_reference_report(
    *,
    recipe: dict[str, Any],
    config: dict[str, Any],
    control_artifact: dict[str, Any],
    palette_artifact: dict[str, Any],
    target_reference: list[dict[str, Any]],
    target_gray_bound: float,
) -> dict[str, Any]:
    material_id = str(recipe["material_id"])
    control_reference = [Decimal(value) for value in control_artifact["reference_vector_decimal"]]
    control_error_bound = float(control_artifact["error_bound"])
    palette_spec = palette_artifact["palette_spec"]
    palette_u8 = np.asarray(palette_spec["palette_u8"], dtype=np.int64)
    palette_decimal = [
        [Decimal(int(channel)) / Decimal(255) for channel in color] for color in palette_u8
    ]
    base_color_decimal = [
        Decimal(str(value)) for value in recipe.get("base_color", [])
    ]
    with localcontext() as context:
        context.prec = 70
        energy = sum((abs(value) for value in control_reference), Decimal(0)) / Decimal(32)
        bias = control_reference[0]
        tint = [
            _decimal_clip(
                base_color_decimal[channel] + Decimal(str(BASE_TINT_SCALE)) * control_reference[channel],
                Decimal(0),
                Decimal(1),
            )
            for channel in range(3)
        ]
        x_scale = Decimal(str(MODULATION_X_SCALE)) + Decimal(str(MODULATION_ENERGY_SCALE)) * energy
    quantized_bound_floor = 0.5 / 255.0
    sample_reports: list[dict[str, Any]] = []
    max_palette_color_distance = 0.0
    palette_array_float = palette_u8.astype(np.float64) / 255.0
    for sample_index, reference in enumerate(target_reference):
        position = reference["position"]
        x_index = int(position["x"])
        y_index = int(position["y"])
        gray_reference = Decimal(reference["reference_decimal"])
        with localcontext() as context:
            context.prec = 70
            modulation_reference = _decimal_clip(
                Decimal("0.5")
                + (gray_reference - Decimal("0.5")) * x_scale
                + Decimal(str(MODULATION_BIAS_SCALE)) * bias,
                Decimal(0),
                Decimal(1),
            )
            target_reference_rgb = [
                tint[channel]
                * (Decimal(str(SHADE_OFFSET)) + Decimal(str(SHADE_SCALE)) * modulation_reference)
                for channel in range(3)
            ]
            best_index = 0
            best_distance: Decimal | None = None
            for palette_index, color in enumerate(palette_decimal):
                distance = sum(
                    (target_reference_rgb[channel] - color[channel]) ** 2 for channel in range(3)
                )
                if best_distance is None or distance < best_distance:
                    best_distance = distance
                    best_index = palette_index
            if best_distance is None:
                raise MatgenError(
                    code=E_NUMERIC,
                    message="palette reference matching had no candidates",
                    material_id=material_id,
                    stage="diagnostics",
                )
        actual_index = int(palette_artifact["palette_indices"][y_index, x_index])
        actual_rgb = np.asarray(palette_artifact["texture_rgba"][y_index, x_index, :3], dtype=np.float64) / 255.0
        reference_rgb_float = palette_u8[best_index].astype(np.float64) / 255.0
        absolute_color_error = float(np.max(np.abs(actual_rgb - reference_rgb_float)))
        if actual_index != best_index:
            index_mismatch_distance = float(
                np.max(np.abs(palette_u8[actual_index].astype(np.float64) - palette_u8[best_index].astype(np.float64)))
                / 255.0
            )
            max_palette_color_distance = max(max_palette_color_distance, index_mismatch_distance)
        sample_bound = (
            target_gray_bound * 0.9
            + 2.0 * control_error_bound
            + quantized_bound_floor
            + max_palette_color_distance
        )
        sample_bound = max(sample_bound, absolute_color_error + 8.0 * EPSILON)
        _check_bound(sample_bound, material_id=material_id, octave_index=None, label="palette_sample_bound")
        sample_reports.append(
            {
                "position": position,
                "actual_palette_index": actual_index,
                "reference_palette_index": best_index,
                "actual_rgb_u8": [int(value) for value in palette_artifact["texture_rgba"][y_index, x_index, :3]],
                "reference_rgb_u8": [int(value) for value in palette_u8[best_index]],
                "reference_gray_decimal": _decimal_text(gray_reference),
                "reference_target_rgb_decimal": [_decimal_text(value) for value in target_reference_rgb],
                "absolute_color_error": absolute_color_error,
                "quantized_channel_error_bound": sample_bound,
            }
        )
    if not sample_reports:
        raise MatgenError(
            code=E_NUMERIC,
            message="no palette samples available for diagnostics",
            material_id=material_id,
            stage="diagnostics",
        )
    max_bound = max(sample["quantized_channel_error_bound"] for sample in sample_reports)
    mismatch_count = sum(
        1
        for sample in sample_reports
        if sample["actual_palette_index"] != sample["reference_palette_index"]
    )
    return {
        "stage": "palette_quantization",
        "sample_count": len(sample_reports),
        "palette_index_mismatch_count": mismatch_count,
        "max_absolute_color_error": max(
            sample["absolute_color_error"] for sample in sample_reports
        ),
        "quantized_channel_error_bound": _check_bound(
            max_bound,
            material_id=material_id,
            octave_index=None,
            label="quantized_channel_error_bound",
        ),
        "effective_digits": significant_digits(0.5, max_bound),
        "error_source": "resampled gray bound -> control modulation -> nearest RGB palette -> 8-bit quantization",
        "samples": sample_reports,
    }


def _material_parameter_report(
    *,
    recipe: dict[str, Any],
    control_artifact: dict[str, Any],
    palette_artifact: dict[str, Any],
    palette_report: dict[str, Any],
) -> dict[str, Any]:
    material_id = str(recipe["material_id"])
    control_reference = [Decimal(value) for value in control_artifact["reference_vector_decimal"]]
    control_bound = float(control_artifact["error_bound"])
    material = palette_artifact["material"]
    with localcontext() as context:
        context.prec = 70
        energy = sum((abs(value) for value in control_reference), Decimal(0)) / Decimal(32)
        roughness_reference = _decimal_clip(
            Decimal(str(recipe["roughness"]))
            + Decimal(str(ROUGHNESS_CONTROL_SCALE)) * control_reference[3]
            + Decimal(str(ROUGHNESS_ENERGY_SCALE)) * (energy - Decimal("0.5")),
            Decimal(0),
            Decimal(1),
        )
        metalness_reference = _decimal_clip(
            Decimal(str(recipe["metalness"]))
            + Decimal(str(METALNESS_CONTROL_SCALE)) * control_reference[4]
            - Decimal(str(METALNESS_ENERGY_SCALE)) * (energy - Decimal("0.5")),
            Decimal(0),
            Decimal(1),
        )

    parameters: dict[str, Any] = {}

    def add_parameter(
        name: str,
        value: float,
        reference: Decimal,
        structural_bound: float,
    ) -> None:
        measured = abs(float(value) - float(reference))
        bound = max(measured + 8.0 * EPSILON, structural_bound)
        bound = _check_bound(bound, material_id=material_id, octave_index=None, label=name)
        assert_finite_report_value(value, material_id=material_id, stage="diagnostics", label=name)
        parameters[name] = {
            "value": float(value),
            "reference_decimal": _decimal_text(reference),
            "absolute_error": measured,
            "error_bound": bound,
            "significant_digits": significant_digits(float(value), bound),
        }

    add_parameter("roughness", float(material["roughness"]), roughness_reference, control_bound * 0.08)
    add_parameter("metalness", float(material["metalness"]), metalness_reference, control_bound * 0.06)
    for channel_index, channel_name in enumerate(("r", "g", "b")):
        add_parameter(
            f"base_color_{channel_name}",
            float(material["base_color"][channel_index]),
            Decimal(str(recipe["base_color"][channel_index])),
            8.0 * EPSILON,
        )

    texture_rgba = np.asarray(palette_artifact["texture_rgba"], dtype=np.uint8)
    height, width = texture_rgba.shape[:2]
    pixel_count = max(1, height * width)
    for channel_index, channel_name in enumerate(("r", "g", "b")):
        integer_sum = int(np.sum(texture_rgba[:, :, channel_index], dtype=np.uint64))
        reference_mean = Decimal(integer_sum) / (Decimal(pixel_count) * Decimal(255))
        add_parameter(
            f"texture_mean_{channel_name}",
            float(material["texture"]["mean_rgb"][channel_index]),
            reference_mean,
            float(palette_report["quantized_channel_error_bound"]),
        )
    return parameters


def _attribution_report(
    *,
    recipe: dict[str, Any],
    octave_reports: list[dict[str, Any]],
    palette_artifact: dict[str, Any],
    target_reference: list[dict[str, Any]],
) -> dict[str, Any]:
    material_id = str(recipe["material_id"])
    ranked_contributions = sorted(
        octave_reports,
        key=lambda entry: (-entry["contribution_bound"], entry["octave_index"]),
    )
    contribution_order = [entry["octave_index"] for entry in ranked_contributions]
    palette_float = np.asarray(palette_artifact["palette_spec"]["palette_float"], dtype=np.float64)
    actual_texture = np.asarray(palette_artifact["texture_rgba"], dtype=np.uint8)
    derived = palette_artifact["derived"]
    tint = np.asarray(derived["tint"], dtype=np.float64)
    base_gray = np.asarray(
        [reference["float_value"] for reference in target_reference], dtype=np.float64
    )
    actual_indices = np.asarray(
        [
            int(
                palette_artifact["palette_indices"][
                    int(reference["position"]["y"]), int(reference["position"]["x"])
                ]
            )
            for reference in target_reference
        ],
        dtype=np.int64,
    )
    actual_colors = palette_float[actual_indices]
    replay_effects: dict[int, float] = {}
    for entry in octave_reports:
        delta = max(1.0e-12, entry["contribution_bound"] * 4.0)
        modulated = np.clip(
            0.5
            + (base_gray + delta - 0.5) * float(derived["gray_x_scale"])
            + MODULATION_BIAS_SCALE * float(derived["bias"]),
            0.0,
            1.0,
        )
        target_rgb = tint[None, :] * (SHADE_OFFSET + SHADE_SCALE * modulated)[:, None]
        target_rgb = np.clip(target_rgb, 0.0, 1.0)[:, None, :]
        replayed_indices = nearest_palette_indices(target_rgb, palette_float)[:, 0]
        replayed_colors = palette_float[replayed_indices]
        color_effect = float(np.mean(np.abs(replayed_colors - actual_colors)))
        changed_fraction = float(np.mean(replayed_indices != actual_indices))
        replay_effects[entry["octave_index"]] = color_effect + 0.01 * changed_fraction
        if not math.isfinite(replay_effects[entry["octave_index"]]):
            raise MatgenError(
                code=E_NUMERIC,
                message="non-finite perturbation replay attribution",
                material_id=material_id,
                stage="diagnostics",
                octave_index=entry["octave_index"],
            )
    replay_order = sorted(
        replay_effects,
        key=lambda octave_index: (-replay_effects[octave_index], octave_index),
    )
    contribution_rank = {octave_index: rank + 1 for rank, octave_index in enumerate(contribution_order)}
    replay_rank = {octave_index: rank + 1 for rank, octave_index in enumerate(replay_order)}
    review_items = []
    for entry in octave_reports:
        octave_index = entry["octave_index"]
        if contribution_rank[octave_index] != replay_rank[octave_index]:
            if replay_effects[octave_index] == 0.0:
                source = "palette/quantization plateau suppresses replay sensitivity"
            else:
                source = "nonlinear nearest-neighbor palette response and 8-bit channel plateau"
            review_items.append(
                {
                    "octave_index": octave_index,
                    "contribution_rank": contribution_rank[octave_index],
                    "replay_rank": replay_rank[octave_index],
                    "contribution_bound": entry["contribution_bound"],
                    "replay_effect": replay_effects[octave_index],
                    "review_required": True,
                    "deviation_source": source,
                }
            )
    return {
        "contribution_order": contribution_order,
        "perturbation_replay_order": replay_order,
        "perturbation_replay_effects": {
            str(octave_index): replay_effects[octave_index] for octave_index in sorted(replay_effects)
        },
        "orders_match": contribution_order == replay_order,
        "review_items": sorted(review_items, key=lambda item: item["octave_index"]),
        "cross_check_method": "error-bound contribution rank vs per-octave +delta palette/quantization replay rank",
    }


def build_diagnostics(
    *,
    recipe: dict[str, Any],
    config: dict[str, Any],
    control_artifact: dict[str, Any],
    noise_artifact: dict[str, Any],
    palette_artifact: dict[str, Any],
) -> dict[str, Any]:
    material_id = str(recipe["material_id"])
    base_positions = deterministic_sample_positions(
        width=int(noise_artifact["base_width"]),
        height=int(noise_artifact["base_height"]),
        sample_count=int(config["sample_count"]),
        sample_seed=int(config["sample_seed"]),
        material_id=material_id,
        purpose="octave-accumulation",
    )
    target_positions = deterministic_sample_positions(
        width=int(noise_artifact["target_width"]),
        height=int(noise_artifact["target_height"]),
        sample_count=int(config["sample_count"]),
        sample_seed=int(config["sample_seed"]),
        material_id=material_id,
        purpose="resample-palette-quantization",
    )
    octave_reports, base_accumulation_bound = _octave_error_report(
        recipe=recipe,
        config=config,
        control_artifact=control_artifact,
        noise_artifact=noise_artifact,
        base_positions=base_positions,
    )
    target_reference = _reference_target_samples(
        recipe=recipe,
        noise_artifact=noise_artifact,
        palette_artifact=palette_artifact,
        target_positions=target_positions,
    )
    max_target_actual_error = max(sample["absolute_error"] for sample in target_reference)
    target_gray_bound = max(
        max_target_actual_error + 8.0 * EPSILON,
        base_accumulation_bound * (1.0 + 1.0e-12) + 16.0 * EPSILON,
    )
    target_gray_bound = _check_bound(
        target_gray_bound,
        material_id=material_id,
        octave_index=None,
        label="target_gray_error_bound",
    )
    resampling_report = {
        "stage": "deterministic_resample",
        "source_resolution": [int(noise_artifact["base_width"]), int(noise_artifact["base_height"])],
        "target_resolution": [int(noise_artifact["target_width"]), int(noise_artifact["target_height"])],
        "method": "corner-aligned bilinear interpolation with decimal reference weights",
        "sample_count": len(target_reference),
        "max_absolute_error": max_target_actual_error,
        "input_accumulation_error_bound": base_accumulation_bound,
        "output_error_bound": target_gray_bound,
        "effective_digits": significant_digits(0.5, target_gray_bound),
        "samples": target_reference,
    }
    palette_report = _palette_reference_report(
        recipe=recipe,
        config=config,
        control_artifact=control_artifact,
        palette_artifact=palette_artifact,
        target_reference=target_reference,
        target_gray_bound=target_gray_bound,
    )
    parameters = _material_parameter_report(
        recipe=recipe,
        control_artifact=control_artifact,
        palette_artifact=palette_artifact,
        palette_report=palette_report,
    )
    attribution = _attribution_report(
        recipe=recipe,
        octave_reports=octave_reports,
        palette_artifact=palette_artifact,
        target_reference=target_reference,
    )

    octave_budget = float(config["octave_error_budget"])
    total_budget = float(config["total_error_budget"])
    over_budget_octaves = [
        entry["octave_index"] for entry in octave_reports if entry["over_budget"]
    ]
    closest_octave = max(
        octave_reports,
        key=lambda entry: (entry["budget_occupancy"], -entry["octave_index"]),
    )
    total_used_bound = target_gray_bound + float(
        palette_report["quantized_channel_error_bound"]
    )
    total_used_bound = _check_bound(
        total_used_bound,
        material_id=material_id,
        octave_index=None,
        label="total_used_bound",
    )
    budget_report = {
        "octave_budget": octave_budget,
        "total_budget": total_budget,
        "over_budget_octaves": over_budget_octaves,
        "closest_octave": {
            "octave_index": closest_octave["octave_index"],
            "budget_occupancy": closest_octave["budget_occupancy"],
            "margin": closest_octave["budget_margin"],
            "over_budget": closest_octave["over_budget"],
        },
        "total_used_bound": total_used_bound,
        "total_budget_occupancy": total_used_bound / total_budget,
        "total_over_budget": total_used_bound > total_budget,
        "total_budget_margin": total_budget - total_used_bound,
    }
    return {
        "schema": DIAGNOSTICS_SCHEMA,
        "material_id": material_id,
        "recipe_fingerprint": fingerprint(recipe),
        "reference_method": {
            "numeric_type": "decimal",
            "precision_digits": 70,
            "accumulation": "explicit per-octave Decimal adds after smoothstep interpolation",
            "sampling": "SplitMix64-selected distinct pixel positions recorded verbatim",
            "missing_information_policy": "error when reference samples or values are unavailable",
        },
        "control": {
            "stage": "control",
            "vector_sha256": control_artifact["control_vector_bytes_sha256"],
            "norm": control_artifact["norm"],
            "norm_error": control_artifact["norm_error"],
            "error_bound": control_artifact["error_bound"],
            "relative_error_bound": control_artifact["relative_error_bound"],
            "significant_digits": control_artifact["significant_digits"],
            "reference_method": control_artifact["reference_method"],
        },
        "sample_positions": {
            "base": base_positions,
            "target": target_positions,
        },
        "octaves": octave_reports,
        "resampling": resampling_report,
        "palette_quantization": palette_report,
        "material_parameters": parameters,
        "error_budget": budget_report,
        "attribution": attribution,
    }

