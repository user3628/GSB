from __future__ import annotations

import numpy as np
import pytest

from image_workbench.algorithms import repair_pixels, style_transfer_pixels
from image_workbench.errors import E_NUMERIC, WorkbenchError
from image_workbench.hashing import digest_array
from image_workbench.numeric import quantize_u8


def test_repair_is_deterministic(fragment_a) -> None:
    mask = fragment_a.mask_pixels
    first = repair_pixels(fragment_a.pixels, mask, 7, fragment_id=fragment_a.fragment_id)
    second = repair_pixels(fragment_a.pixels, mask, 7, fragment_id=fragment_a.fragment_id)
    assert np.array_equal(first.pixels, second.pixels)
    assert digest_array(first.pixels, role="rgb") == digest_array(second.pixels, role="rgb")
    assert first.stats.filled_pixels == int(mask.sum())
    assert first.pixels.dtype == np.uint8
    assert np.array_equal(first.pixels[~mask], fragment_a.pixels[~mask])


def test_style_transfer_is_deterministic(fragment_a, manifest) -> None:
    reference = manifest.fragments[0]
    first = style_transfer_pixels(fragment_a.pixels, reference.pixels, 0.65, fragment_id=fragment_a.fragment_id)
    second = style_transfer_pixels(fragment_a.pixels, reference.pixels, 0.65, fragment_id=fragment_a.fragment_id)
    assert np.array_equal(first.pixels, second.pixels)
    assert first.pixels.dtype == np.uint8
    zero = style_transfer_pixels(fragment_a.pixels, reference.pixels, 0.0, fragment_id=fragment_a.fragment_id)
    assert np.array_equal(zero.pixels, fragment_a.pixels)


def test_invalid_strength_is_numeric_error(fragment_a) -> None:
    with pytest.raises(WorkbenchError) as caught:
        style_transfer_pixels(fragment_a.pixels, fragment_a.pixels, 1.01, fragment_id=fragment_a.fragment_id)
    error = caught.value
    assert error.code == E_NUMERIC
    assert error.fragment_id == fragment_a.fragment_id
    assert error.pixel == [0, 0]


def test_full_mask_without_known_pixels_is_numeric_error(fragment_a) -> None:
    mask = np.ones(fragment_a.pixels.shape[:2], dtype=bool)
    with pytest.raises(WorkbenchError) as caught:
        repair_pixels(fragment_a.pixels, mask, 3, fragment_id=fragment_a.fragment_id)
    assert caught.value.code == E_NUMERIC
    assert caught.value.pixel is not None


def test_quantizer_rejects_non_finite_values() -> None:
    with pytest.raises(WorkbenchError) as caught:
        quantize_u8(np.asarray([[[0.0, np.inf, 2.0]]]), fragment_id="fragment", field="test")
    assert caught.value.code == E_NUMERIC
    assert caught.value.pixel == [0, 0]

