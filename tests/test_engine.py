from __future__ import annotations

import copy
import json
import re

from app.engine import calculate_metrics
from app.snapshot import attach_checksum, parse_snapshot


FIXED_PATTERN = re.compile(r"^-?\d+\.\d{6}$")


def test_engine_is_bit_identical_for_repeated_and_reordered_inputs(baseline_payload: dict) -> None:
    first = calculate_metrics(parse_snapshot(baseline_payload))
    second = calculate_metrics(parse_snapshot(baseline_payload))
    assert json.dumps(first, sort_keys=True) == json.dumps(second, sort_keys=True)

    reordered_payload = copy.deepcopy(baseline_payload)
    reordered_payload["components"] = {
        key: reordered_payload["components"][key]
        for key in reversed(list(reordered_payload["components"]))
    }
    reordered_payload = attach_checksum(reordered_payload)
    third = calculate_metrics(parse_snapshot(reordered_payload))
    assert json.dumps(first, sort_keys=True) == json.dumps(third, sort_keys=True)


def test_engine_metrics_and_tolerance_contract(baseline_payload: dict) -> None:
    result = calculate_metrics(parse_snapshot(baseline_payload))["metrics"]
    assert result["conclusion"]["status"] == "CONSISTENT"
    assert result["weighted_consistency"]["value"] == "1.000000"
    assert result["all_error_bounds_covered"] is True
    assert result["precision_contract"]["rounding_mode"] == "ROUND_HALF_EVEN"
    assert result["conclusion"]["relative_tolerance"] == "0.000010"
    assert result["conclusion"]["tie_break"] == "component_id_lexicographic"
    assert result["conclusion"]["critical_component"] == "api"
    for component in result["components"].values():
        for metric_name in ("quota_share", "headroom_ratio", "limit_utilization"):
            assert FIXED_PATTERN.match(component[metric_name]["value"])
            assert FIXED_PATTERN.match(component[metric_name]["cumulative_error"])


def test_error_explanation_contains_every_rounding_step(adjusted_payload: dict) -> None:
    calculated = calculate_metrics(parse_snapshot(adjusted_payload))
    explanation = calculated["explanation"]
    assert explanation["rounding"]["operation_count"] > 20
    assert len(explanation["rounding"]["events"]) == explanation["rounding"]["operation_count"]
    assert all(event["rounding_mode"] in ("ROUND_HALF_EVEN", "EXACT") for event in explanation["rounding"]["events"])
    assert explanation["sum_stability"]["canonical_order"] == ["api", "cache", "worker"]
    assert explanation["sum_stability"]["integer_sum_error_bound"] == "0.000000"
    assert explanation["comparison_contract"]["error_bounds_are_reported_separately"] is True


def test_single_component_change_cascades_to_weighted_consistency(
    baseline_payload: dict, adjusted_payload: dict
) -> None:
    before = calculate_metrics(parse_snapshot(baseline_payload))["metrics"]
    after = calculate_metrics(parse_snapshot(adjusted_payload))["metrics"]
    assert before["components"]["api"]["quota_share"]["value"] == "0.400000"
    assert after["components"]["api"]["quota_share"]["value"] == "0.393939"
    assert before["weighted_consistency"]["value"] == "1.000000"
    assert after["weighted_consistency"]["value"] == "0.914142"
    assert after["components"]["worker"]["quota_share"]["value"] == "0.353535"
    assert after["components"]["cache"]["share_score"]["value"] == "0.949500"
    assert after["conclusion"]["status"] == "CONSISTENT"


def test_out_of_band_change_is_a_deterministic_failure(baseline_payload: dict) -> None:
    changed = copy.deepcopy(baseline_payload)
    changed["components"]["api"]["quota"] = 500
    changed = attach_checksum(changed)
    result = calculate_metrics(parse_snapshot(changed))["metrics"]
    assert result["components"]["api"]["quota_share"]["value"] == "0.454545"
    assert result["components"]["api"]["quota_share"]["inside"] is False
    assert result["conclusion"]["status"] == "INCONSISTENT"
    assert result["conclusion"]["critical_component"] == "api"
    assert any(check["check"] == "quota_share_interval" and not check["ok"] for check in result["conclusion"]["failed_checks"])
