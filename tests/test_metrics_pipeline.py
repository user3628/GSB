from __future__ import annotations

from pathlib import Path

from vadkit.metrics import evaluation_fingerprint, match_endpoints
from vadkit.pipeline import evaluate_dataset


def test_tolerance_controls_matching_and_fingerprint() -> None:
    references = [{"start_ms": 100.0, "end_ms": 300.0}]
    predictions = [{"start_ms": 110.0, "end_ms": 290.0}]
    strict = match_endpoints(references, predictions, 5.0)
    loose = match_endpoints(references, predictions, 20.0)
    assert strict.hits == 0 and strict.misses == 1 and strict.false_positives == 1
    assert loose.hits == 1 and loose.mean_deviation_ms == 10.0
    assert evaluation_fingerprint("config-a", 5.0) != evaluation_fingerprint("config-a", 20.0)


def test_summary_is_byte_reproducible_and_dual_channel(dataset_dir: Path, tmp_path: Path) -> None:
    first = evaluate_dataset(
        dataset_dir / "manifest.json", tmp_path / "run-a", tolerance_ms=80.0
    )
    second = evaluate_dataset(
        dataset_dir / "manifest.json", tmp_path / "run-b", tolerance_ms=80.0
    )
    assert first.status == "complete" and second.status == "complete"
    assert first.result_sha256 == second.result_sha256
    assert first.result_path.read_bytes() == second.result_path.read_bytes()
    assert first.result["batch_stream_consistent"]
    assert first.result["aggregate"]["batch"] == first.result["aggregate"]["stream"]
    assert first.result["aggregate"]["batch"]["hits"] == 5
    assert first.result["aggregate"]["batch"]["false_positives"] == 0
    assert first.result["aggregate"]["batch"]["misses"] == 0
    assert first.result["aggregate"]["batch"]["hit_rate"] == 1.0


def test_tolerance_change_changes_metrics_and_evaluation_fingerprint(
    dataset_dir: Path, tmp_path: Path
) -> None:
    workspace = tmp_path / "tolerance-run"
    loose = evaluate_dataset(dataset_dir / "manifest.json", workspace, tolerance_ms=80.0)
    strict = evaluate_dataset(dataset_dir / "manifest.json", workspace, tolerance_ms=1.0)
    assert loose.result["evaluation_fingerprint"] != strict.result["evaluation_fingerprint"]
    assert loose.result["aggregate"]["batch"]["hit_rate"] != strict.result["aggregate"]["batch"]["hit_rate"]
    assert strict.result["aggregate"]["batch"]["misses"] > 0

