from __future__ import annotations

from pathlib import Path

from image_workbench.demo_data import generate_demo_data


def test_generator_is_deterministic(sample_dir: Path, tmp_path: Path) -> None:
    first = generate_demo_data(tmp_path / "first")
    second = generate_demo_data(tmp_path / "second")
    assert first["sample_digest"] == second["sample_digest"]
    assert first["fragment_count"] >= 4
    assert set(first["formats"]) == {"png", "jpeg"}
    for record in first["images"].values():
        assert Path(tmp_path / "first" / record["path"]).read_bytes() == Path(tmp_path / "second" / record["path"]).read_bytes()


def test_committed_samples_match_generator(sample_dir: Path) -> None:
    generated = generate_demo_data(sample_dir)
    assert (sample_dir / "manifest.json").exists()
    assert (sample_dir / "legacy_manifest.json").exists()
    assert generated["sample_digest"]

