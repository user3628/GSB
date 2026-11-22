from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from mmbackfill.canonical import read_jsonl
from mmbackfill.demo import generate_demo_dataset
from mmbackfill.engine import Engine
from mmbackfill.errors import EngineError
from mmbackfill.workspace import Workspace
from tests.conftest import make_engine


def test_vectors_are_deterministic_and_unit_normalized(tmp_path: Path, demo_dataset: Path) -> None:
    first = make_engine(tmp_path / "one", demo_dataset)
    second = make_engine(tmp_path / "two", demo_dataset)
    result_a = first.submit_batch(batch_id="a", text_ids=["txt-a", "txt-b"], expected_sequence=0, order="id")
    result_b = second.submit_batch(batch_id="b", text_ids=["txt-a", "txt-b"], expected_sequence=0, order="arrival")
    assert first.workspace.vectors_path.read_bytes() == second.workspace.vectors_path.read_bytes()
    assert first.workspace.index_path.read_bytes() == second.workspace.index_path.read_bytes()
    cache = first.vectors()
    for item in cache["vectors"].values():
        vector = np.asarray(item["vector"], dtype=np.float64)
        assert vector.shape == (280,)
        assert abs(float(np.linalg.norm(vector)) - 1.0) < 1e-9
    assert result_a["version_fingerprints"]["vector_build"] == result_b["version_fingerprints"]["vector_build"]
    follow_up = first.submit_batch(batch_id="follow", text_ids=["txt-c"], expected_sequence=1, order="id")
    assert follow_up["stages"]["vector_build"]["reused_vectors"] == 2


def test_valid_injected_vector_is_persisted_and_recoverable(tmp_path: Path, demo_dataset: Path) -> None:
    engine = make_engine(tmp_path / "override", demo_dataset)
    injected = [0.01 * (index + 1) for index in range(280)]
    result = engine.submit_batch(
        batch_id="override",
        text_ids=["txt-a"],
        expected_sequence=0,
        order="id",
        vector_overrides={"txt-a": injected},
    )
    assert result["batch_seq"] == 1
    expected = engine.workspace.index_path.read_bytes()
    engine.workspace.state_path.unlink()
    engine.workspace.index_path.unlink()
    recovered = engine.recover(clear_stop=True)
    assert recovered["last_batch_seq"] == 1
    assert engine.workspace.index_path.read_bytes() == expected


def test_missing_png_empty_text_and_zero_fusion_are_diagnostic_errors(tmp_path: Path, demo_dataset: Path) -> None:
    engine = make_engine(tmp_path / "missing", demo_dataset)
    missing_image = {"image_id": "img-missing-png", "file": "absent.png", "language": "zh"}
    missing_text = {
        "text_id": "txt-missing-png",
        "text": "中文 missing / png",
        "image_id": "img-missing-png",
        "target_shard": "s0",
    }
    engine.import_inputs(read_jsonl(demo_dataset / "images.jsonl") + [missing_image], [missing_text], demo_dataset / "images")
    with pytest.raises(EngineError) as caught:
        engine.submit_batch(batch_id="missing-png", text_ids=["txt-missing-png"], expected_sequence=0, order="id")
    assert caught.value.code == "E_EMPTY_INPUT"
    assert caught.value.entries == [{"text_id": "txt-missing-png", "image_id": "img-missing-png"}]
    assert engine.status()["last_batch_seq"] == 0
    assert any(item["error"]["code"] == "E_EMPTY_INPUT" for item in engine.diagnostics()["diagnostics"])

    empty_engine = make_engine(tmp_path / "empty-text", demo_dataset)
    empty_engine.import_texts(
        [
            {
                "text_id": "txt-empty",
                "text": "  \n\t ",
                "image_id": "img-red-blue",
                "target_shard": "s0",
            }
        ]
    )
    with pytest.raises(EngineError) as empty_caught:
        empty_engine.submit_batch(batch_id="empty-text", text_ids=["txt-empty"], expected_sequence=0, order="id")
    assert empty_caught.value.code == "E_EMPTY_INPUT"
    assert empty_engine.status()["last_batch_seq"] == 0

    zero_engine = Engine(Workspace(tmp_path / "zero-fusion"))
    zero_engine.import_inputs(read_jsonl(demo_dataset / "images.jsonl"), read_jsonl(demo_dataset / "texts.jsonl"), demo_dataset / "images")
    zero_layout = {
        "schema_version": "layout-v1",
        "layout_id": "zero-fusion",
        "vector": {
            "dimension": 280,
            "fusion": {"image_weight": 0.0, "text_weight": 0.0},
            "visual": {"grid_rows": 4, "grid_cols": 4, "edge_bins": 8},
            "text": {"dimension": 128},
        },
        "layers": [{"layer": 0, "max_vectors": 4, "max_shards": 4}],
        "shards": [{"shard_id": "s0", "layer": 0, "capacity": 2, "fallbacks": []}],
    }
    zero_engine.install_layout(zero_layout)
    with pytest.raises(EngineError) as zero_caught:
        zero_engine.submit_batch(batch_id="zero-fusion", text_ids=["txt-a"], expected_sequence=0, order="id")
    assert zero_caught.value.code == "E_EMPTY_INPUT"
    assert zero_engine.status()["last_batch_seq"] == 0


def test_png_roundtrip_and_text_normalization(tmp_path: Path) -> None:
    from mmbackfill.png import decode_rgb_png_bytes, encode_rgb_png
    from mmbackfill.vectors import normalize_text

    pixels = np.zeros((9, 13, 3), dtype=np.uint8)
    pixels[:, :, 1] = 180
    encoded = encode_rgb_png(pixels)
    decoded = decode_rgb_png_bytes(encoded)
    assert np.array_equal(decoded, pixels)
    assert normalize_text("  ＲＥＤ\tred\n中文  ") == "red red 中文"
