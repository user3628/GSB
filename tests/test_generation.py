from __future__ import annotations

import json
from pathlib import Path

from gridpipe.generator import generate_samples


def test_generation_is_byte_deterministic(tmp_path: Path) -> None:
    first = generate_samples(tmp_path / "first", seed=20261122)
    second = generate_samples(tmp_path / "second", seed=20261122)
    assert first == second
    assert (tmp_path / "first" / "manifest.json").read_bytes() == (
        tmp_path / "second" / "manifest.json"
    ).read_bytes()
    for item in first["images"]:
        assert (tmp_path / "first" / item["path"]).read_bytes() == (
            tmp_path / "second" / item["path"]
        ).read_bytes()


def test_generated_manifest_declares_required_coverage(tmp_path: Path) -> None:
    summary = generate_samples(tmp_path / "data", seed=20261122)
    manifest = json.loads((tmp_path / "data" / "manifest.json").read_text(encoding="utf-8"))
    assert len(manifest["shards"]) >= 4
    kinds = {shard["truth"]["chart"]["kind"] for shard in manifest["shards"]}
    assert kinds == {"bar", "line"}
    assert any(shard["truth"]["table"]["merges"] for shard in manifest["shards"])
    assert all(shard["truth"]["table"]["text_samples"] for shard in manifest["shards"])
    assert summary["content_sha256"] == generate_samples(tmp_path / "again", seed=20261122)[
        "content_sha256"
    ]
