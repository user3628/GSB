from __future__ import annotations

from pathlib import Path

import numpy as np

from image_workbench.legacy import (
    LegacyConfig,
    export_upgraded_manifest,
    upgrade_legacy_manifest,
)
from image_workbench.manifest import load_manifest


def test_legacy_upgrade_is_stable_and_semantically_equivalent(sample_dir: Path, tmp_path: Path) -> None:
    first, document = upgrade_legacy_manifest(sample_dir / "legacy_manifest.json")
    second, _ = upgrade_legacy_manifest(sample_dir / "legacy_manifest.json")
    assert [item.fragment_id for item in first.fragments] == [item.fragment_id for item in second.fragments]
    business = next(item for item in first.fragments if item.fragment_id != first.fragments[0].fragment_id)
    expected = np.zeros((20, 20), dtype=bool)
    expected[5:10, 5:10] = True
    assert np.array_equal(business.mask_pixels, expected)
    assert business.style_strength == 0.6
    assert business.metadata["source_schema_version"] == 1
    assert document["schema_version"] == 2
    assert all("id" in fragment for fragment in document["fragments"])

    exported = tmp_path / "upgraded_v2.json"
    export_upgraded_manifest(first, exported)
    reloaded = load_manifest(exported)
    assert [item.fragment_id for item in reloaded.fragments] == [item.fragment_id for item in first.fragments]
    assert np.array_equal(reloaded.fragments[1].mask_pixels, expected)
    assert reloaded.fragments[1].style_strength == 0.6


def test_legacy_config_controls_prefix_and_strength(sample_dir: Path) -> None:
    config = LegacyConfig(id_prefix="old", strength_percent_base=100.0, coordinate_decimals=9)
    upgrade, document = upgrade_legacy_manifest(sample_dir / "legacy_manifest.json", config)
    assert all(item.fragment_id.startswith("old-") for item in upgrade.fragments)
    assert upgrade.fragments[1].style_strength == 0.6
    assert document["fragments"][1]["style_strength"] == 0.6


def test_legacy_config_can_interpret_bottom_left_coordinates(sample_dir: Path) -> None:
    config = LegacyConfig(id_prefix="bottom", coordinate_origin="bottom-left")
    upgrade, document = upgrade_legacy_manifest(sample_dir / "legacy_manifest.json", config)
    business = upgrade.fragments[1]
    expected = np.zeros((20, 20), dtype=bool)
    expected[10:15, 5:10] = True
    assert np.array_equal(business.mask_pixels, expected)
    assert document["fragments"][1]["mask"]["rects"] == business.mask.to_lists()

