from __future__ import annotations

import json
from pathlib import Path

import pytest

from panopipe.errors import E_INVALID_CONFIG, E_NOT_FOUND, E_UNSUPPORTED_SCHEMA, PipelineError
from panopipe.manifest import ParseOptions, load_manifest, parse_manifest
from panopipe.semantic import decode_semantic, merge_instances


OPTIONS = ParseOptions(
    default_instance_start_id=100,
    min_instance_area=4,
    confidence_threshold=0.6,
    mask_format="png_palette",
)


def test_v1_normalization_and_upgrade_equivalence(demo_dir: Path) -> None:
    legacy = load_manifest(demo_dir / "manifest_v1.json", options=OPTIONS)
    upgraded = load_manifest(demo_dir / "manifest_v1_upgraded.json", options=OPTIONS)
    assert legacy.schema_version == 1
    assert upgraded.schema_version == 2
    assert [s.shard_id for s in legacy.shards] == [s.shard_id for s in upgraded.shards]
    for left, right in zip(legacy.shards, upgraded.shards):
        assert left.instance_start_id == right.instance_start_id == 100
        assert left.confidence_threshold == pytest.approx(0.6)
        assert left.min_instance_area == right.min_instance_area == 4
        assert [(c.name, c.color, c.category_id, c.thing) for c in left.categories] == [
            (c.name, c.color, c.category_id, c.thing) for c in right.categories
        ]
        left_model = merge_instances(decode_semantic(left.image_path, left), left)
        right_model = merge_instances(decode_semantic(right.image_path, right), right)
        assert left_model.digest() == right_model.digest()


def test_v1_argb_and_percent_are_normalized(tmp_path: Path) -> None:
    data = {
        "schema_version": 1,
        "project": "mini",
        "shards": [
            {
                "id": "s",
                "image": "unused.png",
                "categories": {"road": "#7F112233", "person": "#FF445566"},
                "confidence_threshold": 75,
            }
        ],
    }
    manifest = parse_manifest(data, base_dir=tmp_path, options=OPTIONS)
    shard = manifest.shards[0]
    assert shard.instance_start_id == 100
    assert shard.confidence_threshold == pytest.approx(0.75)
    assert shard.category_by_name["road"].color == (0x11, 0x22, 0x33)
    assert shard.category_by_name["road"].thing is False
    assert shard.category_by_name["person"].thing is True
    v2 = json.loads(manifest.to_json(version=2))
    assert v2["schema_version"] == 2
    v2_categories = {item["name"]: item for item in v2["shards"][0]["categories"]}
    assert v2_categories["road"]["color"] == "#112233"


def test_v2_downgrade_contains_v1_shape(demo_dir: Path) -> None:
    manifest = load_manifest(demo_dir / "manifest_v2.json")
    value = manifest.to_dict(version=1)
    shard = value["shards"][0]
    assert "instance_start_id" not in shard
    assert shard["confidence_threshold"] == 60
    assert all(color.startswith("#FF") and len(color) == 9 for color in shard["categories"].values())


def test_unknown_schema_is_rejected(demo_dir: Path) -> None:
    with pytest.raises(PipelineError) as exc_info:
        load_manifest(demo_dir / "errors" / "unknown_schema_manifest.json")
    assert exc_info.value.code == E_UNSUPPORTED_SCHEMA
    assert exc_info.value.details["supported"] == [1, 2]


def test_empty_categories_and_instance_start_conflict(tmp_path: Path) -> None:
    empty = {
        "schema_version": 2,
        "shards": [
            {
                "id": "s",
                "image": "s.png",
                "categories": [],
                "instance_start_id": 100,
            }
        ],
    }
    with pytest.raises(PipelineError) as exc_info:
        parse_manifest(empty, base_dir=tmp_path)
    assert exc_info.value.code == E_INVALID_CONFIG

    conflict = {
        "schema_version": 2,
        "shards": [
            {
                "id": "s",
                "image": "s.png",
                "categories": [{"id": 9, "name": "x", "color": "#010203"}],
                "instance_start_id": 9,
            }
        ],
    }
    with pytest.raises(PipelineError) as exc_info:
        parse_manifest(conflict, base_dir=tmp_path)
    assert exc_info.value.code == E_INVALID_CONFIG
    assert exc_info.value.details["max_category_id"] == 9


def test_missing_manifest_invalid_hex_and_v1_default_threshold(tmp_path: Path) -> None:
    with pytest.raises(PipelineError) as exc_info:
        load_manifest(tmp_path / "missing.json")
    assert exc_info.value.code == E_NOT_FOUND

    invalid_color = {
        "schema_version": 1,
        "shards": [
            {
                "id": "s",
                "image": "s.png",
                "categories": {"x": "#ZZ112233"},
            }
        ],
    }
    with pytest.raises(PipelineError) as exc_info:
        parse_manifest(invalid_color, base_dir=tmp_path, options=OPTIONS)
    assert exc_info.value.code == E_INVALID_CONFIG

    no_threshold = {
        "schema_version": 1,
        "shards": [
            {
                "id": "s",
                "image": "s.png",
                "categories": {"road": "#FF112233"},
            }
        ],
    }
    manifest = parse_manifest(no_threshold, base_dir=tmp_path, options=OPTIONS)
    assert manifest.shards[0].confidence_threshold == pytest.approx(0.6)
