from __future__ import annotations

import json
from pathlib import Path

from sampling_engine.cli import main


def test_fixed_demo_digest_and_stop_resume(tmp_path):
    expected_path = Path("artifacts/demo_expected.json")
    expected = json.loads(expected_path.read_text(encoding="utf-8"))
    output_path = tmp_path / "demo_result.json"
    code = main(
        [
            "demo",
            "--workspace",
            str(tmp_path / "demo"),
            "--output",
            str(output_path),
            "--expected",
            str(expected_path),
        ]
    )
    assert code == 0
    result = json.loads(output_path.read_text(encoding="utf-8"))
    assert result["verification"]["expected_match"] is True
    assert result["verification"]["stop_resume_equal"] is True
    assert result["verification"]["checkpoint_sequences_contiguous"] is True
    assert result["full_run"]["result_digest"] == expected["result_digest"]
    assert result["verification"]["successful_requests"] == 5
    assert result["verification"]["failed_requests"] == 2


def test_demo_data_contains_required_coverage():
    metadata = json.loads(Path("data/generator_metadata.json").read_text(encoding="utf-8"))
    assert metadata["request_count"] >= 4
    assert metadata["small_vocab_size"] <= 32
    assert metadata["large_vocab_size"] >= 256
    assert metadata["files"]["r2_nan"]["has_nan"] is True
    assert metadata["files"]["r2_inf"]["has_positive_inf"] is True
    v1 = json.loads(Path("data/manifest_v1.json").read_text(encoding="utf-8"))
    v2 = json.loads(Path("data/manifest_v2.json").read_text(encoding="utf-8"))
    assert v1["manifest_version"] == 1
    assert all("top_p" not in request for request in v1["requests"])
    assert any(
        any("\u4e00" <= char <= "\u9fff" for char in token)
        for token in v2["vocab"]
    )
    assert any(token.isascii() and token.isalpha() for token in v2["vocab"])
