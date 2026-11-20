"""Build deterministic local JSON fixtures and their canonical checksums."""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from capacity_ledger.checksum import compute_payload_checksum  # noqa: E402


SAMPLES = ROOT / "samples"
INCREMENTS = SAMPLES / "increments"


def service(
    *,
    replicas: int,
    cores: int,
    memory: int,
    min_cores: int,
    max_cores: int,
    min_memory: int,
    max_memory: int,
    core_price: int,
    memory_price: int,
    budget: int,
    max_share_ppm: int,
    slo_ppm: int,
) -> dict[str, int]:
    return {
        "replicas": replicas,
        "cores_per_replica": cores,
        "memory_gb_per_replica": memory,
        "min_cores": min_cores,
        "max_cores": max_cores,
        "min_memory_gb": min_memory,
        "max_memory_gb": max_memory,
        "price_core_cents_per_core_hour": core_price,
        "price_memory_cents_per_gb_hour": memory_price,
        "budget_cents_per_hour": budget,
        "max_cost_share_ppm": max_share_ppm,
        "slo_target_ppm": slo_ppm,
    }


BASE_SERVICES: dict[str, dict[str, int]] = {
    "api-gateway": service(
        replicas=4,
        cores=2,
        memory=4,
        min_cores=1,
        max_cores=8,
        min_memory=1,
        max_memory=16,
        core_price=12,
        memory_price=3,
        budget=180,
        max_share_ppm=150_000,
        slo_ppm=990_000,
    ),
    "auth-service": service(
        replicas=3,
        cores=2,
        memory=2,
        min_cores=1,
        max_cores=4,
        min_memory=1,
        max_memory=8,
        core_price=15,
        memory_price=4,
        budget=160,
        max_share_ppm=120_000,
        slo_ppm=995_000,
    ),
    "catalog-service": service(
        replicas=3,
        cores=4,
        memory=8,
        min_cores=2,
        max_cores=8,
        min_memory=4,
        max_memory=16,
        core_price=10,
        memory_price=2,
        budget=200,
        max_share_ppm=120_000,
        slo_ppm=980_000,
    ),
    "cart-service": service(
        replicas=2,
        cores=2,
        memory=4,
        min_cores=1,
        max_cores=4,
        min_memory=2,
        max_memory=12,
        core_price=12,
        memory_price=3,
        budget=100,
        max_share_ppm=100_000,
        slo_ppm=970_000,
    ),
    "checkout-service": service(
        replicas=3,
        cores=4,
        memory=6,
        min_cores=2,
        max_cores=8,
        min_memory=4,
        max_memory=16,
        core_price=18,
        memory_price=4,
        budget=340,
        max_share_ppm=250_000,
        slo_ppm=999_000,
    ),
    "payments-service": service(
        replicas=2,
        cores=4,
        memory=8,
        min_cores=2,
        max_cores=8,
        min_memory=4,
        max_memory=24,
        core_price=22,
        memory_price=5,
        budget=320,
        max_share_ppm=200_000,
        slo_ppm=999_500,
    ),
    "search-service": service(
        replicas=5,
        cores=8,
        memory=16,
        min_cores=4,
        max_cores=16,
        min_memory=8,
        max_memory=32,
        core_price=14,
        memory_price=3,
        budget=900,
        max_share_ppm=450_000,
        slo_ppm=990_000,
    ),
    "notifications-service": service(
        replicas=2,
        cores=1,
        memory=2,
        min_cores=1,
        max_cores=2,
        min_memory=1,
        max_memory=4,
        core_price=8,
        memory_price=2,
        budget=60,
        max_share_ppm=100_000,
        slo_ppm=950_000,
    ),
}


def signed(payload: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(payload)
    result.pop("checksum", None)
    result["checksum"] = compute_payload_checksum(result)
    return result


def snapshot_payload() -> dict[str, Any]:
    payload = {
        "kind": "snapshot",
        "commit_id": "commit-v1-baseline",
        "version": 1,
        "generated_at": "2026-01-15T08:00:00Z",
        "total_budget_cents_per_hour": 2100,
        "tolerance_ppm": 1000,
        "services": copy.deepcopy(BASE_SERVICES),
    }
    return signed(payload)


def increment_payload(
    *,
    filename: str,
    commit_id: str,
    base_version: int,
    generated_at: str,
    service_name: str,
    changes: dict[str, int],
) -> dict[str, Any]:
    updated = copy.deepcopy(BASE_SERVICES[service_name])
    updated.update(changes)
    payload = {
        "kind": "increment",
        "commit_id": commit_id,
        "base_version": base_version,
        "version": base_version + 1,
        "generated_at": generated_at,
        "services": {service_name: updated},
    }
    return signed(payload)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def main() -> None:
    snapshot = snapshot_payload()
    write_json(SAMPLES / "snapshot_v1.json", snapshot)

    inc1 = increment_payload(
        filename="auth_price",
        commit_id="commit-v2-auth-price",
        base_version=1,
        generated_at="2026-01-15T08:05:00Z",
        service_name="auth-service",
        changes={"price_core_cents_per_core_hour": 20},
    )
    write_json(INCREMENTS / "01_auth_price_increment.json", inc1)

    inc2 = increment_payload(
        filename="catalog_cores",
        commit_id="commit-v3-catalog-cores",
        base_version=2,
        generated_at="2026-01-15T08:10:00Z",
        service_name="catalog-service",
        changes={"cores_per_replica": 5},
    )
    write_json(INCREMENTS / "02_catalog_cores_increment.json", inc2)

    violation = increment_payload(
        filename="payments_violation",
        commit_id="commit-v4-payments-violation",
        base_version=3,
        generated_at="2026-01-15T08:15:00Z",
        service_name="payments-service",
        changes={"replicas": 4},
    )
    write_json(INCREMENTS / "03_payments_budget_violation.json", violation)

    crash = increment_payload(
        filename="cart_crash",
        commit_id="commit-v4-cart-crash",
        base_version=3,
        generated_at="2026-01-15T08:20:00Z",
        service_name="cart-service",
        changes={"memory_gb_per_replica": 6},
    )
    write_json(INCREMENTS / "04_cart_force_terminate.json", crash)

    slo_full = increment_payload(
        filename="notifications_slo",
        commit_id="commit-v4-notifications-slo",
        base_version=3,
        generated_at="2026-01-15T08:25:00Z",
        service_name="notifications-service",
        changes={"slo_target_ppm": 960_000},
    )
    write_json(INCREMENTS / "05_notifications_slo_full_rebuild.json", slo_full)

    expected = {
        "snapshot": {
            "version": 1,
            "total_cost_cents_per_hour": 1866,
            "verdict": "PASS",
            "artifact_count": 69,
        },
        "increments": [
            {
                "file": "01_auth_price_increment.json",
                "version": 2,
                "mode": "incremental",
                "total_cost_cents_per_hour": 1896,
                "verdict": "PASS",
            },
            {
                "file": "02_catalog_cores_increment.json",
                "version": 3,
                "mode": "incremental",
                "total_cost_cents_per_hour": 1926,
                "verdict": "PASS",
            },
            {
                "file": "03_payments_budget_violation.json",
                "version": 4,
                "mode": "rejected",
                "error": "E_BUDGET_EXCEEDED",
            },
            {
                "file": "04_cart_force_terminate.json",
                "version": 4,
                "mode": "aborted",
                "error": "E_PROCESS_TERMINATED",
            },
            {
                "file": "05_notifications_slo_full_rebuild.json",
                "version": 4,
                "mode": "full",
                "reason": "weighted-cost normalization denominator changed",
                "total_cost_cents_per_hour": 1926,
                "verdict": "PASS",
            },
        ],
    }
    write_json(SAMPLES / "expected.json", expected)
    print(f"wrote fixed samples under {SAMPLES}")


if __name__ == "__main__":
    main()

