from __future__ import annotations

import copy
import math
from pathlib import Path

import pytest

from xrel_workbench.config import load_config
from xrel_workbench.errors import BusinessError, E_NUMERIC
from xrel_workbench.scoring import score_pair
from xrel_workbench.util import read_json, read_jsonl


def _stage2_pair(workspace, pair_id: str = "p0001") -> dict:
    for path in sorted((workspace.artifacts / "stage2").glob("*.jsonl")):
        for record in read_jsonl(path):
            if record["pair_id"] == pair_id:
                return record
    raise AssertionError(f"pair not found: {pair_id}")


def test_bridge_mapping_and_fixed_point_metadata(workspace) -> None:
    record = _stage2_pair(workspace, "p0001")
    assert record["candidates"]
    assert any("bridge" in candidate["routes"] for candidate in record["candidates"])
    quantization = record["quantization"]
    assert quantization["bits"] == 12
    assert quantization["scale_factor"] > 0
    assert quantization["rounding"] == "round_half_even"
    for candidate in record["candidates"]:
        term_quantization = candidate["quantization"]
        assert term_quantization["scale_factor"] == quantization["scale_factor"]
        assert abs(term_quantization["weight_quantized"] - candidate["weight_raw"]) <= (
            term_quantization["absolute_error_bound"] + 1e-15
        )


def test_log_score_clipping_and_source_contributions(workspace) -> None:
    config = load_config(workspace.scoring)
    records = []
    for path in sorted((workspace.artifacts / "stage2").glob("*.jsonl")):
        records.extend(read_jsonl(path))
    scores = [score_pair(record, config) for record in records]
    clipped = [item for item in scores if item["clipped"]]
    assert clipped
    assert all(item["score"] == item["score_clip"] for item in clipped)
    for item in scores:
        source_sum = math.fsum(item["source_log_contributions"].values())
        assert source_sum == pytest.approx(item["score_unclipped"], abs=1e-12)
        for contribution in item["contributions"]:
            contribution_sum = math.fsum(contribution["source_log_contributions"].values())
            assert contribution_sum == pytest.approx(contribution["log_contribution"], abs=1e-12)


@pytest.mark.parametrize("mutation", ["negative", "nan", "inf", "zero_scale"])
def test_numeric_failures_are_explicit(workspace, mutation: str) -> None:
    config = load_config(workspace.scoring)
    record = copy.deepcopy(_stage2_pair(workspace, "p0001"))
    candidate = record["candidates"][0]
    if mutation == "negative":
        candidate["quantization"]["q"] = -1
        candidate["quantization"]["weight_quantized"] = -1.0 / candidate["quantization"]["scale_factor"]
    elif mutation == "nan":
        candidate["quantization"]["weight_quantized"] = float("nan")
    elif mutation == "inf":
        candidate["quantization"]["weight_quantized"] = float("inf")
    else:
        candidate["quantization"]["scale_factor"] = 0
    with pytest.raises(BusinessError) as raised:
        score_pair(record, config)
    error = raised.value
    assert error.code == E_NUMERIC
    assert error.details["pair_id"] == "p0001"
    assert error.details["term"]
    assert error.details["stage"] == "stage3_log_domain_score"


def test_error_report_uses_explicit_scale_and_high_precision_reference(workspace) -> None:
    report = read_json(workspace.reports / "error_report.json")
    validation = read_json(workspace.reports / "validation.json")
    assert report["pair_count"] == 24
    assert validation["error_crosscheck_review_pairs"] == []
    for pair in report["pairs"]:
        detail = pair["error_report"]
        assert detail["reference_method"] == "decimal_ln1p_sum_v1"
        assert detail["crosscheck_ok"] is True
        assert detail["bound_path_ok"] is True
        assert detail["term_bound_conflicts"] == []
        assert detail["effective_digits"] >= 0
        bound = float(detail["analytical_error_bound"])
        observed = float(detail["observed_quantization_error"])
        assert observed <= bound + 1e-12
        for term in detail["per_term"]:
            assert float(term["scale_factor"]) > 0
            assert term["rounding"] == "round_half_even"


def test_grading_and_confusion_report(workspace) -> None:
    validation = read_json(workspace.reports / "validation.json")
    assert len(validation["grade_counts"]) == 4
    matrix = validation["confusion_matrix"]["counts"]
    assert len(matrix) == 4 and all(len(row) == 4 for row in matrix)
    assert sum(sum(row) for row in matrix) == validation["labeled_pair_count"]
    assert validation["out_of_bounds_samples"]
    assert validation["suggestions"]
    assert validation["error_propagation"]["method"] == "least_squares_cumulative_shard_max_bound_v1"
