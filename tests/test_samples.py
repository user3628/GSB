from __future__ import annotations

import json
from pathlib import Path

from linreg_workbench.canonical import sha256_file
from linreg_workbench.samples import generate_samples

ROOT = Path(__file__).resolve().parent.parent


def test_bundled_declaration_matches_files() -> None:
    declaration = json.loads((ROOT / "data" / "dataset_declaration.json").read_text("utf-8"))
    assert [item["dataset_id"] for item in declaration["datasets"]] == ["small", "medium"]
    small, medium = declaration["datasets"]
    assert (small["rows"], small["columns"], len(small["features"])) == (600, 12, 11)
    assert (medium["rows"], medium["columns"], len(medium["features"])) == (6000, 40, 39)
    assert len(medium["missing_columns"]) == 5
    assert len(medium["redundant_columns"]) == 9
    for dataset in declaration["datasets"]:
        path = ROOT / "data" / dataset["file"]
        assert sha256_file(path) == dataset["sha256"]
        assert dataset["class_counts"].keys() == {"0", "1"}


def test_generator_is_byte_deterministic(tmp_path: Path) -> None:
    declaration = generate_samples(tmp_path)
    for item in declaration["datasets"]:
        name = item["file"]
        assert (tmp_path / name).read_bytes() == (ROOT / "data" / name).read_bytes()
    assert (tmp_path / "dataset_declaration.json").read_bytes() == (
        ROOT / "data" / "dataset_declaration.json"
    ).read_bytes()
    assert (tmp_path / "DATASETS.md").read_bytes() == (
        ROOT / "data" / "DATASETS.md"
    ).read_bytes()
