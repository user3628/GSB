from __future__ import annotations

import numpy as np
import pytest

from matgen.errors import E_DIM_MISMATCH, E_PALETTE_INVALID, MatgenError
from matgen.palette import (
    load_palette,
    nearest_palette_indices,
    validate_quantized_texture,
)


def base_recipe(**overrides) -> dict:
    recipe = {
        "material_id": "palette_material",
        "base_color": [0.4, 0.5, 0.6],
        "roughness": 0.5,
        "metalness": 0.2,
        "resolution": [8, 6],
        "noise": {"seed": 1, "octaves": [{"frequency": 1.0, "amplitude": 1.0}]},
    }
    recipe.update(overrides)
    return recipe


def test_reference_card_palette_is_unique_and_sorted(samples_dir) -> None:
    palette = load_palette(base_recipe(palette_card="earth"), cards_path=samples_dir / "color_cards.json")
    colors = [tuple(int(value) for value in row) for row in palette["palette_u8"]]
    assert colors == sorted(set(colors))
    assert palette["metadata"]["palette_card"] == "earth"
    assert palette["source_fingerprint"]


def test_fallback_palette_depends_only_on_base_color() -> None:
    first = load_palette(base_recipe(), cards_path=None)
    second = load_palette(base_recipe(), cards_path=None)
    assert np.array_equal(first["palette_u8"], second["palette_u8"])
    assert first["source_fingerprint"] == second["source_fingerprint"]


def test_empty_and_dimension_mismatch_cards_return_business_codes(samples_dir) -> None:
    with pytest.raises(MatgenError) as empty:
        load_palette(base_recipe(palette_card="empty"), cards_path=samples_dir / "color_cards.json")
    assert empty.value.code == E_PALETTE_INVALID
    assert empty.value.material_id == "palette_material"

    with pytest.raises(MatgenError) as mismatch:
        load_palette(base_recipe(palette_card="broken_size"), cards_path=samples_dir / "color_cards.json")
    assert mismatch.value.code == E_DIM_MISMATCH
    assert mismatch.value.material_id == "palette_material"


def test_illegal_quantized_channels_are_rejected() -> None:
    illegal = np.asarray([[[256, 0, 0, 255]]], dtype=np.int32)
    with pytest.raises(MatgenError) as captured:
        validate_quantized_texture(illegal, material_id="bad_texture")
    assert captured.value.code == E_PALETTE_INVALID
    assert captured.value.material_id == "bad_texture"


def test_nearest_palette_respects_first_index_tie_break() -> None:
    palette = np.asarray([[0.2, 0.2, 0.2], [0.8, 0.8, 0.8], [0.2, 0.2, 0.2]], dtype=np.float64)
    target = np.asarray([[[0.2, 0.2, 0.2]]], dtype=np.float64)
    assert int(nearest_palette_indices(target, palette)[0, 0]) == 0

