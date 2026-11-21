from __future__ import annotations

import math

import numpy as np
import pytest

from matgen.config import normalize_config
from matgen.errors import E_NUMERIC, E_OCTAVE_RANGE, MatgenError
from matgen.noise import bilinear_resample, compose_noise, value_noise_octave


def recipe(octave_count: int = 4) -> dict:
    return {
        "material_id": "noise_material",
        "resolution": [13, 9],
        "noise": {
            "seed": 104729,
            "base_resolution": [17, 11],
            "octaves": [
                {"frequency": float(index + 1), "amplitude": 0.7 / (index + 1)}
                for index in range(octave_count)
            ],
        },
    }


def test_single_and_multithreaded_octave_synthesis_are_identical() -> None:
    config = normalize_config({"max_threads": 4})
    serial = compose_noise(recipe(6), config, max_threads=1)
    threaded = compose_noise(recipe(6), config, max_threads=4)
    assert np.array_equal(serial["field"], threaded["field"])
    assert np.max(np.abs(serial["field"] - threaded["field"])) <= 1e-12
    for serial_octave, threaded_octave in zip(serial["octave_fields"], threaded["octave_fields"]):
        assert np.array_equal(serial_octave, threaded_octave)


def test_noise_is_bounded_and_amplitude_normalized() -> None:
    result = compose_noise(recipe(5), normalize_config())
    assert np.all(result["field"] >= 0.0)
    assert np.all(result["field"] <= 1.0)
    assert sum(result["normalized_amplitudes"]) == pytest.approx(1.0, abs=1e-16)
    assert result["base_width"] == 17 and result["base_height"] == 11
    assert result["target_width"] == 13 and result["target_height"] == 9


def test_boundary_interpolation_and_resample_are_deterministic() -> None:
    first = value_noise_octave(1234, 2, 4.0, 9, 13)
    second = value_noise_octave(1234, 2, 4.0, 9, 13)
    assert np.array_equal(first, second)
    assert first[0, 0] == pytest.approx(value_noise_octave(1234, 2, 4.0, 9, 13)[0, 0])
    resampled = bilinear_resample(first, 15, 21)
    assert resampled.shape == (15, 21)
    assert np.array_equal(resampled, bilinear_resample(first, 15, 21))
    assert np.all(np.isfinite(resampled))


def test_octave_count_and_nan_errors_are_material_scoped() -> None:
    with pytest.raises(MatgenError) as captured:
        compose_noise(recipe(0), normalize_config())
    assert captured.value.code == E_OCTAVE_RANGE
    assert captured.value.material_id == "noise_material"

    invalid = recipe(3)
    invalid["noise"]["octaves"][1]["amplitude"] = math.nan
    with pytest.raises(MatgenError) as captured_nan:
        compose_noise(invalid, normalize_config())
    assert captured_nan.value.code == E_NUMERIC
    assert captured_nan.value.material_id == "noise_material"
    assert captured_nan.value.octave_index == 1

