from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from capacity_ledger.checksum import canonical_json
from capacity_ledger.errors import CapacityError
from capacity_ledger.service import CapacityLedger

from conftest import (
    final_v3_services,
    load_json,
    sign,
    snapshot_from_services,
)


def no_float(value: object) -> bool:
    if isinstance(value, float):
        return False
    if isinstance(value, dict):
        return all(no_float(item) for item in value.values())
    if isinstance(value, list):
        return all(no_float(item) for item in value)
    return True


def test_exact_service_costs_and_fixed_output(sample_root: Path, ledger: CapacityLedger) -> None:
    result = ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    metrics = result["metrics"]
    assert metrics["aggregate"]["total_cost_cents_per_hour"] == 1866
    assert metrics["aggregate"]["total_cost_cents_per_hour_fixed"] == "1866.000000"
    assert metrics["services"]["search-service"]["cost_cents_per_hour"] == 800
    assert metrics["services"]["api-gateway"]["cost_cents_per_hour"] == 144
    assert metrics["services"]["notifications-service"]["cost_cents_per_hour"] == 24
    assert result["verdict"]["status"] == "PASS"
    assert no_float(metrics)

    steps = {step["artifact"]: step for step in metrics["rounding"]["steps"]}
    assert steps["service_cost"]["method"] == "EXACT_INTEGER_MULTIPLY_ADD"
    assert steps["service_share"]["method"] == "ROUND_HALF_UP"
    assert steps["budget_decisions"]["method"] == "EXACT_CROSS_MULTIPLY_RELATIVE_TOLERANCE"
    assert steps["service_share"]["cumulative_error_ppm"] == metrics["rounding"][
        "cumulative_rounding_error_ppm"
    ]
    for service in metrics["services"].values():
        assert len(service["cost_cents_per_hour_fixed"].split(".")[1]) == 6
        assert len(service["share_fixed"].split(".")[1]) == 6


def test_weighted_index_uses_exact_rational_and_half_up(sample_root: Path, ledger: CapacityLedger) -> None:
    result = ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    aggregate = result["metrics"]["aggregate"]
    services = result["metrics"]["services"]
    numerator = sum(
        service["cost_cents_per_hour"] * service["slo_target_ppm"]
        for service in services.values()
    )
    denominator = sum(service["slo_target_ppm"] for service in services.values())
    assert aggregate["weighted_cost_numerator"] == numerator
    assert aggregate["weight_denominator_ppm"] == denominator
    exact_scaled = numerator * 1_000_000
    quotient, remainder = divmod(exact_scaled, denominator)
    expected = quotient + (1 if remainder * 2 >= denominator else 0)
    assert aggregate["weighted_cost_index_micro_cents"] == expected
    assert aggregate["weighted_cost_index_exact_fraction"] == f"{exact_scaled}/{denominator}"
    weighted_step = next(
        step
        for step in result["metrics"]["rounding"]["steps"]
        if step["artifact"] == "weighted_cost_index"
    )
    assert weighted_step["error_numerator"] == expected * denominator - exact_scaled


def test_tolerance_boundary_is_not_a_violation(sample_root: Path, ledger: CapacityLedger) -> None:
    base = load_json(sample_root / "snapshot_v1.json")
    service = copy.deepcopy(base["services"]["notifications-service"])
    service.update(
        {
            "replicas": 1,
            "cores_per_replica": 1,
            "memory_gb_per_replica": 1,
            "min_cores": 1,
            "max_cores": 1,
            "min_memory_gb": 1,
            "max_memory_gb": 1,
            "price_core_cents_per_core_hour": 1000,
            "price_memory_cents_per_gb_hour": 1,
            "budget_cents_per_hour": 1000,
            "max_cost_share_ppm": 1_000_000,
            "slo_target_ppm": 1_000_000,
        }
    )
    payload = snapshot_from_services(
        {"edge-service": service}, total_budget=1000, tolerance_ppm=1000
    )
    result = ledger.commit_snapshot(payload)
    assert result["metrics"]["aggregate"]["total_cost_cents_per_hour"] == 1001
    assert result["verdict"]["status"] == "PASS"
    assert {item["constraint"] for item in result["verdict"]["within_tolerance"]} == {
        "service_budget:edge-service",
        "total_budget",
    }

    violating = copy.deepcopy(service)
    violating["price_core_cents_per_core_hour"] = 1001
    payload = snapshot_from_services(
        {"edge-service": violating},
        version=2,
        commit_id="test-snapshot-violating",
        total_budget=1000,
        tolerance_ppm=1000,
    )
    with pytest.raises(CapacityError) as exc_info:
        ledger.commit_snapshot(payload)
    assert exc_info.value.code == "E_BUDGET_EXCEEDED"
    assert exc_info.value.details["primary_constraint"] == "service_budget:edge-service"
    assert ledger.get_current()["version"] == 1


def test_local_path_and_full_path_are_bit_identical(sample_root: Path, tmp_path: Path) -> None:
    local = CapacityLedger(tmp_path / "local.sqlite3", tmp_path / "local-intents")
    base = load_json(sample_root / "snapshot_v1.json")
    local.commit_snapshot(base)
    local.commit_increment(load_json(sample_root / "increments" / "01_auth_price_increment.json"))
    local_result = local.commit_increment(
        load_json(sample_root / "increments" / "02_catalog_cores_increment.json")
    )
    assert local_result["mode"] == "incremental"

    full = CapacityLedger(tmp_path / "full.sqlite3", tmp_path / "full-intents")
    full_payload = snapshot_from_services(
        final_v3_services(sample_root),
        version=1,
        commit_id="full-parity-v3",
        total_budget=base["total_budget_cents_per_hour"],
        tolerance_ppm=base["tolerance_ppm"],
    )
    full_result = full.commit_snapshot(full_payload)
    assert full_result["mode"] == "full"
    assert canonical_json(local_result["metrics"]) == canonical_json(full_result["metrics"])
    assert canonical_json(local_result["verdict"]) == canonical_json(full_result["verdict"])


def test_slo_change_selects_full_rebuild(sample_root: Path, ledger: CapacityLedger) -> None:
    ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    ledger.commit_increment(load_json(sample_root / "increments" / "01_auth_price_increment.json"))
    ledger.commit_increment(load_json(sample_root / "increments" / "02_catalog_cores_increment.json"))
    result = ledger.commit_increment(
        load_json(sample_root / "increments" / "05_notifications_slo_full_rebuild.json")
    )
    assert result["mode"] == "full"
    assert result["rebuild_scope"]["full_rebuild"] is True
    assert result["rebuild_scope"]["rebuilt_count"] == result["rebuild_scope"]["artifact_count"]
    assert "normalization denominator" in result["mode_reason"]
    assert result["metrics"]["aggregate"]["total_cost_cents_per_hour"] == 1926


def test_budget_violation_is_sorted_and_does_not_commit(sample_root: Path, ledger: CapacityLedger) -> None:
    ledger.commit_snapshot(load_json(sample_root / "snapshot_v1.json"))
    ledger.commit_increment(load_json(sample_root / "increments" / "01_auth_price_increment.json"))
    before = ledger.commit_increment(
        load_json(sample_root / "increments" / "02_catalog_cores_increment.json")
    )
    with pytest.raises(CapacityError) as exc_info:
        ledger.commit_increment(
            load_json(sample_root / "increments" / "03_payments_budget_violation.json")
        )
    assert exc_info.value.code == "E_BUDGET_EXCEEDED"
    details = exc_info.value.details
    constraints = [item["constraint"] for item in details["violations"]]
    assert constraints == sorted(constraints)
    assert "service_budget:payments-service" in constraints
    assert "service_cost_share:payments-service" in constraints
    assert "total_budget" in constraints
    assert details["persisted_state_changed"] is False
    assert details["violations"][0]["observed_cents_per_hour_fixed"].endswith("000000")
    current = ledger.get_current()
    assert current["version"] == 3
    assert current["ledger_checksum"] == before["ledger_checksum"]


def test_snapshot_checksum_and_spec_validation(sample_root: Path, ledger: CapacityLedger) -> None:
    payload = load_json(sample_root / "snapshot_v1.json")
    payload["services"]["auth-service"]["cores_per_replica"] = 3
    with pytest.raises(CapacityError) as exc_info:
        ledger.commit_snapshot(payload)
    assert exc_info.value.code == "E_SNAPSHOT_INVALID"

    payload = load_json(sample_root / "snapshot_v1.json")
    payload["services"]["auth-service"]["cores_per_replica"] = 99
    payload = sign(payload)
    with pytest.raises(CapacityError) as exc_info:
        ledger.commit_snapshot(payload)
    assert exc_info.value.code == "E_SNAPSHOT_INVALID"
    assert ledger.get_current()["version"] == 0

