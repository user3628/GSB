"""Fixed-sample seeding and demonstration report construction."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .clock import FixedClock
from .metrics import numerical_stability_benchmark
from .service import LeaseService
from .time_contract import ttl_precision_report


def load_sample(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def seed_fixed_scenario(
    db_path: str | Path, sample_path: str | Path
) -> tuple[LeaseService, FixedClock, dict[str, Any]]:
    """Seed the fixed sample through the public service API.

    The operation sequence is deterministic and recreates every persisted
    grant/expiry timestamp in the JSON fixture.
    """

    sample = load_sample(sample_path)
    origin = int(sample["origin_ms"])
    clock = FixedClock(origin - 5000)
    service = LeaseService(db_path, clock=clock)
    for key in sample["registered_keys"]:
        service.register_key(key)

    beta = service.acquire(
        "beta",
        request_id="sample-beta-grant",
        holder_id="holder-b",
        ttl_ms=30000,
        weight="0.35",
    )
    service.renew(
        "beta",
        request_id="sample-beta-renew-1",
        holder_id="holder-b",
        version=int(beta["lease"]["version"]),
        ttl_ms=30000,
    )
    clock.advance(4000)
    acquired = service.acquire(
        "alpha",
        request_id="sample-alpha-grant",
        holder_id="holder-a",
        ttl_ms=7200000,
        weight="0.45",
    )
    version = int(acquired["lease"]["version"])
    service.renew(
        "alpha",
        request_id="sample-alpha-renew-1",
        holder_id="holder-a",
        version=version,
        ttl_ms=7200000,
    )
    service.renew(
        "alpha",
        request_id="sample-alpha-renew-2",
        holder_id="holder-a",
        version=version,
        ttl_ms=7200000,
    )
    service.acquire(
        "delta",
        request_id="sample-delta-grant",
        holder_id="holder-a",
        ttl_ms=300000,
        weight="0.45",
    )
    clock.advance(800)
    service.acquire(
        "gamma",
        request_id="sample-gamma-grant",
        holder_id="holder-c",
        ttl_ms=300,
        weight="0.2",
    )
    clock.now = origin
    return service, clock, sample


def build_demo_report(
    service: LeaseService,
    clock: FixedClock,
    sample: dict[str, Any],
) -> dict[str, Any]:
    origin = int(sample["origin_ms"])
    metrics_before = service.metrics(now_ms=origin)
    metrics_repeat = service.metrics(now_ms=origin)
    persisted_matches = True
    for expected in sample["leases"]:
        lock = service.get_lock(expected["lock_key"], now_ms=origin)
        lease = lock.get("lease") or {}
        persisted_matches = persisted_matches and all(
            (
                lease.get("holder_id") == expected["holder_id"],
                lease.get("granted_at_ms") == expected["granted_at_ms"],
                lease.get("expires_at_ms") == expected["expires_at_ms"],
                lease.get("ttl_ms") == expected["ttl_ms"],
                lease.get("renew_count") == expected["renew_count"],
                lease.get("weight_input") == expected["weight"],
            )
        )
    arbitration_forward = service.arbitrate(
        sample["free_key_for_arbitration"],
        candidates=sample["candidates"],
        now_ms=origin,
    )
    arbitration_reverse = service.arbitrate(
        sample["free_key_for_arbitration"],
        candidates=list(reversed(sample["candidates"])),
        now_ms=origin,
    )
    grant = service.acquire_best(
        sample["free_key_for_arbitration"],
        request_id="sample-best-grant",
        candidates=sample["candidates"],
    )
    metrics_after = service.metrics(now_ms=origin)
    time_contract = ttl_precision_report(sample["ttl_precision_samples_ms"], now_ms=origin)
    numerical = sample["numerical_benchmark"]
    numeric_report = {
        "normalized_holder_shares": numerical_stability_benchmark(
            numerical["share_terms_q"], numerical["ttl_values_ms"]
        ),
        "mixed_magnitude_stress": numerical_stability_benchmark(
            numerical["stress_terms_q"], numerical["ttl_values_ms"]
        ),
        "comparison_protocol": {
            "canonical": "Q9 integers plus Kahan/Decimal compensated sum",
            "naive": "Python sum over binary floats (audit only)",
            "error_bound": "gamma_(n-1)*sum|x_i| for naive; 2u*sum|x_i| for Kahan; exact Q integer canonical",
        },
    }
    invariants = {
        "repeat_metrics_bit_identical": metrics_before == metrics_repeat,
        "fixed_sample_matches_persisted_rows": persisted_matches,
        "arbitration_order_independent": arbitration_forward["arbitration"]
        == arbitration_reverse["arbitration"],
        "occupied_keys": metrics_before["cluster"]["occupied_keys"],
        "registered_keys": metrics_before["cluster"]["registered_keys"],
        "occupancy": metrics_before["cluster"]["occupancy"]["decimal"],
        "rebalance_suggestion_count": metrics_before["rebalance"]["suggestion_count"],
        "arbitration_winner": arbitration_forward["arbitration"]["winner"]["holder_id"],
        "granted_holder": grant["lease"]["holder_id"],
        "metrics_after_occupancy": metrics_after["cluster"]["occupancy"]["decimal"],
    }
    return {
        "sample": sample["name"],
        "fixed_point": metrics_before["fixed_point"],
        "grant_trajectory": service.events(limit=50)["events"],
        "metrics_before_arbitration": {
            "cluster": metrics_before["cluster"],
            "holders": metrics_before["holders"],
            "weighted_fairness": metrics_before["weighted_fairness"],
            "rebalance": metrics_before["rebalance"],
            "rounding": metrics_before["rounding"],
            "numerical_cross_check": metrics_before["numerical_cross_check"],
        },
        "arbitration": arbitration_forward["arbitration"],
        "grant_result": grant,
        "metrics_after_arbitration": {
            "cluster": metrics_after["cluster"],
            "holders": metrics_after["holders"],
            "weighted_fairness": metrics_after["weighted_fairness"],
            "rebalance": metrics_after["rebalance"],
        },
        "time_contract": time_contract,
        "numerical_error_report": numeric_report,
        "invariants": invariants,
    }

