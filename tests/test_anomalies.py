from __future__ import annotations

import json
from pathlib import Path

import pytest

from matgen.config import load_config
from matgen.errors import MatgenError
from matgen.pipeline import generate_all

from conftest import SAMPLES, write_recipes


def test_bundled_injected_anomalies_return_expected_business_codes(tmp_path: Path) -> None:
    recipes = json.loads((SAMPLES / "invalid_recipes.json").read_text(encoding="utf-8"))
    expected = json.loads((SAMPLES / "expected_errors.json").read_text(encoding="utf-8"))
    expected_codes = {case["material_id"]: case["code"] for case in expected["cases"]}
    config = load_config(SAMPLES / "config.json")
    for recipe in recipes:
        case_root = tmp_path / recipe["material_id"]
        recipe_path = write_recipes(case_root / "recipe.json", [recipe])
        with pytest.raises(MatgenError) as captured:
            generate_all(
                workspace=case_root,
                recipes_path=recipe_path,
                cards_path=SAMPLES / "color_cards.json",
                config=config,
                output_root="out",
            )
        assert captured.value.code == expected_codes[recipe["material_id"]]
        assert captured.value.material_id == recipe["material_id"]
        if recipe["material_id"] == "nan_noise":
            assert captured.value.octave_index == 1

