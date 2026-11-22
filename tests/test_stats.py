from __future__ import annotations

from pathlib import Path

import pytest

from diaglab.errors import BusinessError
from diaglab.formats import decode_bundle
from diaglab.normalize import normalize_bundle
from diaglab.stats import compare_statistics, compute_path_stats, root_cause_candidates


SAMPLES = Path(__file__).resolve().parents[1] / "samples"


def records():
    return normalize_bundle(decode_bundle((SAMPLES / "binary_v2.bin").read_bytes())).records


def test_loss_rate_and_nearest_rank_are_declared_and_stable():
    stats_a = compute_path_stats("local-edge-a", records())
    stats_b = compute_path_stats("local-edge-a", records())
    assert stats_a["loss"]["percent"] == "12.50"
    assert stats_a["loss"]["fraction"] == "0.125000"
    assert stats_a["loss"]["rounding"].startswith("ROUND_HALF_UP")
    assert stats_a["rtt_us"]["sample_count"] == 7
    assert stats_a["rtt_us"]["quantile_method"] == "nearest-rank"
    # Sorted RTTs: 1200,1250,1260,1310,1380,1500,4500; p50 rank ceil(3.5)=4.
    assert stats_a["rtt_us"]["p50"] == 1310
    assert stats_a["calculation_digest"] == stats_b["calculation_digest"]


def test_compensated_sum_bound_and_jitter_contract():
    stats = compute_path_stats("local-edge-b", records())
    cumulative = stats["rtt_us"]["cumulative"]
    assert cumulative["absolute_error_us"] <= cumulative["error_upper_bound_us"]
    assert stats["jitter_us"]["pair_count"] == 4
    assert stats["jitter_us"]["mean_abs_successive_us"] == "162.50"
    assert "definition" in stats["jitter_us"]


def test_insufficient_samples_returns_required_and_actual_without_zero_quantiles():
    stats = compute_path_stats("local-edge-a", records(), min_samples=8, allow_insufficient=True)
    # Root-cause ranking may still use loss/failure evidence, but RTT fields stay null.
    with pytest.raises(BusinessError) as caught:
        compute_path_stats("local-edge-a", records(), min_samples=8)
    assert caught.value.code == "E_INSUFFICIENT_SAMPLES"
    assert caught.value.details["required_samples"] == 8
    assert caught.value.details["actual_samples"] == 7
    assert stats["sample"]["quantile_status"] == "E_INSUFFICIENT_SAMPLES"
    assert stats["rtt_us"]["p95"] is None
    assert stats["jitter_us"]["mean_abs_successive_us"] is None


def test_root_cause_order_uses_numeric_loss_jitter_and_failure_types():
    result = root_cause_candidates(records())
    assert result["candidates"][0]["path_node"] == "local-edge-b"
    assert result["candidates"][0]["rank_basis"]["loss_fraction"] == pytest.approx(2 / 7)
    assert result["candidates"][0]["rank_basis"]["failure_types"] == {"refused": 2}
    scores = [float(candidate["rank_score"]) for candidate in result["candidates"]]
    assert scores == sorted(scores, reverse=True)
    assert result["calculation_digest"]


def test_compare_statistics_declares_relative_tolerance():
    records_a = records()
    stats_a = compute_path_stats("local-edge-a", records_a)
    stats_b = compute_path_stats("local-edge-c", records_a)
    result = compare_statistics(stats_a, stats_b, relative_tolerance=1e-6)
    assert result["tolerance"]["relative"] == 1e-6
    assert result["comparisons"]["loss_fraction_equal"] is False
    assert result["comparisons"]["p95_equal"] is False


def test_window_is_start_inclusive_end_exclusive():
    source = records()
    stats = compute_path_stats(
        "local-edge-a",
        source,
        start_ms=1_704_067_200_000,
        end_ms=1_704_067_203_000,
        min_samples=1,
    )
    # Three local-edge-a records at +0,+1,+2 seconds.
    assert stats["sample"]["total"] == 3

