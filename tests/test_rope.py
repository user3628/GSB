from __future__ import annotations

import math

import numpy as np

from rope_diag.config import RopeConfig
from rope_diag.digest import digest_arrays
from rope_diag.rope import build_frequency_artifact, rotate_tensors


def test_standard_rope_rotation_matches_closed_form() -> None:
    config = RopeConfig(base_frequency=10000.0, rotation_block_size=2)
    query = np.array([[[[1.0, 0.0, 1.0, 0.0]]]])
    key = query.copy()
    frequency = build_frequency_artifact(1, 4, config)
    rotated = rotate_tensors(query, key, frequency["arrays"]["angle_table"], config)
    expected = np.array(
        [
            [
                [
                    [
                        math.cos(0.0),
                        math.sin(0.0),
                        math.cos(0.0),
                        math.sin(0.0),
                    ]
                ]
            ]
        ]
    )
    np.testing.assert_allclose(rotated["query_rotated"], expected, rtol=0, atol=0)
    np.testing.assert_array_equal(rotated["key_rotated"], rotated["query_rotated"])


def test_odd_head_dimension_last_channel_is_unrotated_and_recorded() -> None:
    config = RopeConfig(rotation_block_size=4)
    query = np.arange(1 * 1 * 2 * 5, dtype=np.float64).reshape(1, 1, 2, 5) + 1
    key = query * 2
    frequency = build_frequency_artifact(2, 5, config)
    rotated = rotate_tensors(query, key, frequency["arrays"]["angle_table"], config)
    assert frequency["metadata"]["unrotated_channels"] == [4]
    np.testing.assert_array_equal(rotated["query_rotated"][..., 4], query[..., 4])
    np.testing.assert_array_equal(rotated["key_rotated"][..., 4], key[..., 4])
    assert frequency["arrays"]["rotated_mask"].tolist() == [True, True, True, True, False]


def test_rotation_is_byte_deterministic_and_chunk_invariant() -> None:
    rng = np.random.default_rng(7)
    query = rng.standard_normal((2, 3, 15, 10))
    key = rng.standard_normal((2, 3, 15, 10))
    config = RopeConfig(scaling_mode="ntk_piecewise", scaling_factor=3.0, rotation_block_size=4)
    frequency = build_frequency_artifact(15, 10, config)
    first = rotate_tensors(
        query, key, frequency["arrays"]["angle_table"], config, batch_chunk_size=2
    )
    second = rotate_tensors(
        query, key, frequency["arrays"]["angle_table"], config, batch_chunk_size=1
    )
    assert digest_arrays(first) == digest_arrays(second)
    third = rotate_tensors(
        query, key, frequency["arrays"]["angle_table"], config, batch_chunk_size=2
    )
    assert digest_arrays(first) == digest_arrays(third)


def test_linear_scaling_divides_inverse_frequency() -> None:
    base = RopeConfig(scaling_mode="none", scaling_factor=1.0)
    linear = RopeConfig(scaling_mode="linear", scaling_factor=4.0)
    standard = build_frequency_artifact(8, 8, base)["arrays"]["inverse_frequencies"]
    scaled = build_frequency_artifact(8, 8, linear)["arrays"]["inverse_frequencies"]
    np.testing.assert_array_equal(scaled, standard / 4.0)


def test_ntk_piecewise_is_piecewise_and_repeatable() -> None:
    config = RopeConfig(scaling_mode="ntk_piecewise", scaling_factor=4.0)
    first = build_frequency_artifact(12, 16, config)
    second = build_frequency_artifact(12, 16, config)
    assert digest_arrays(first["arrays"]) == digest_arrays(second["arrays"])
    frequencies = first["arrays"]["inverse_frequencies"]
    standard = build_frequency_artifact(
        12, 16, RopeConfig(scaling_mode="none")
    )["arrays"]["inverse_frequencies"]
    assert not np.array_equal(frequencies, standard)
    assert not np.array_equal(frequencies, standard / 4.0)
    assert first["arrays"]["angle_table"].shape == (12, 16)
