from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def test_manifest_shapes_digests_and_required_coverage():
    manifest = json.loads((DATA / "manifest.json").read_text(encoding="utf-8"))
    records = manifest["samples"]
    names = {record["name"] for record in records}
    assert "small_64x64x64" in names
    assert "medium_512x256x384" in names
    assert "large_1024x1024x1024" in names
    assert any(record["mode"] == "bmm" for record in records)
    assert any(record["contains_nonfinite"] for record in records)
    assert {(record["trans_a"], record["trans_b"]) for record in records} >= {
        ("N", "N"),
        ("T", "N"),
        ("N", "T"),
        ("T", "T"),
    }
    for record in records:
        a_path = DATA / record["a_file"]
        b_path = DATA / record["b_file"]
        a = np.load(a_path, allow_pickle=False)
        b = np.load(b_path, allow_pickle=False)
        assert list(a.shape) == record["a_shape"]
        assert list(b.shape) == record["b_shape"]
        assert a.dtype == np.float64 and b.dtype == np.float64
        assert _sha256(a_path) == record["a_sha256"]
        assert _sha256(b_path) == record["b_sha256"]

