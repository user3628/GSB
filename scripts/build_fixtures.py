from __future__ import annotations

import json
import random
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
RANDOM_SEED = 20261122
BASE_TIMESTAMP_MS = 1764000000000

SERVICE_PATHS = {
    "catalog": ("catalog-service", "/api/catalog/items/{item_id}", ["GET", "GET", "GET", "POST"]),
    "checkout": ("checkout-service", "/api/checkout/orders/{order_id}", ["POST", "POST", "GET"]),
    "payment": ("payment-service", "/api/payment/charges/{charge_id}", ["POST", "GET"]),
    "search": ("search-service", "/api/search/results/{query_id}", ["GET", "GET"]),
}


def status_for(rng: random.Random, service: str) -> int:
    roll = rng.random()
    if service == "checkout":
        if roll < 0.08:
            return 503
        if roll < 0.13:
            return 500
        return 200 if roll < 0.9 else 201
    if service == "catalog":
        if roll < 0.04:
            return 404
        return 200 if roll < 0.92 else 304
    if service == "payment":
        if roll < 0.06:
            return 402
        return 200 if roll < 0.93 else 202
    if service == "search":
        if roll < 0.05:
            return 429
        return 200 if roll < 0.94 else 204
    return 200


def make_record(rng: random.Random, connection_id: str, timestamp_ms: int, service: str) -> dict:
    destination, path, methods = SERVICE_PATHS[service]
    status_code = status_for(rng, service)
    success = status_code < 400
    base_bytes = {
        "catalog": 900,
        "checkout": 1400,
        "payment": 1100,
        "search": 2400,
    }[service]
    base_duration = {
        "catalog": 7200,
        "checkout": 21000,
        "payment": 16000,
        "search": 9500,
    }[service]
    response_bytes = max(0, int(rng.gauss(base_bytes if success else base_bytes // 3, base_bytes / 5)))
    duration_us = max(200, int(rng.gauss(base_duration * (1.3 if status_code >= 500 else 1.0), base_duration / 4)))
    return {
        "timestamp_ms": timestamp_ms,
        "connection_id": connection_id,
        "source_service": "edge-gateway",
        "destination_service": destination,
        "method": rng.choice(methods),
        "path_template": path,
        "status_code": status_code,
        "response_bytes": response_bytes,
        "duration_us": duration_us,
    }


def build_primary_records() -> list[dict]:
    rng = random.Random(RANDOM_SEED)
    records: list[dict] = []
    services = ["catalog", "checkout", "payment", "search"]
    for connection_index in range(1, 46):
        connection_id = f"conn-{connection_index:04d}"
        timestamp = BASE_TIMESTAMP_MS + connection_index * 1000
        service = services[(connection_index - 1) % len(services)]
        for request_index in range(4):
            records.append(make_record(rng, connection_id, timestamp, service))
            timestamp += 18 + request_index * 9
    for connection_index in range(1, 9):
        connection_id = f"internal-{connection_index:04d}"
        timestamp = BASE_TIMESTAMP_MS + 47000 + connection_index * 100
        records.append(
            {
                "timestamp_ms": timestamp,
                "connection_id": connection_id,
                "source_service": "mesh-sidecar",
                "destination_service": "observability-service",
                "method": "GET",
                "path_template": "/internal/metrics/{target}",
                "status_code": 200,
                "response_bytes": 128,
                "duration_us": 1800,
            }
        )
    return records


def build_increment_records() -> list[dict]:
    rng = random.Random(RANDOM_SEED + 1)
    records = []
    start = BASE_TIMESTAMP_MS + 60000
    for index in range(1, 13):
        connection_id = f"increment-conn-{index:04d}"
        service = "checkout" if index % 3 == 0 else "catalog" if index % 2 else "search"
        records.append(make_record(rng, connection_id, start + index * 500, service))
    return records


def write_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


def main() -> None:
    DATA.mkdir(parents=True, exist_ok=True)
    primary = build_primary_records()
    increment = build_increment_records()
    write_jsonl(DATA / "mirror_records.jsonl", primary)
    write_jsonl(DATA / "mirror_records_increment.jsonl", increment)
    policy = {
        "policy_version": "p-baseline-1",
        "seed": RANDOM_SEED,
        "sampling_rate": 0.55,
        "service_weights": {
            "catalog-service": 0.8,
            "checkout-service": 1.2,
            "payment-service": 0.4,
            "search-service": 0.9,
            "observability-service": 1.0,
        },
        "include_path_templates": ["/api/*"],
        "exclude_path_templates": ["/api/payment/*", "/internal/*"],
        "dedup_window_ms": 90,
        "replay_multiplier": 2,
        "max_batch_samples": 8,
        "max_batch_concurrency": 4,
        "batch_max_bytes": 200000,
        "batch_max_duration_us": 350000,
        "replay_timeout_ms": 600,
    }
    (DATA / "policy_baseline.json").write_text(
        json.dumps(policy, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    weight_patch = {
        "base_policy_version": "p-baseline-1",
        "policy_version": "p-checkout-weight-2",
        "patch": {"service_weights": {"checkout-service": 0.2}},
    }
    (DATA / "policy_increment_checkout_weight.json").write_text(
        json.dumps(weight_patch, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    dedup_patch = {
        "base_policy_version": "p-checkout-weight-2",
        "policy_version": "p-dedup-window-3",
        "patch": {"dedup_window_ms": 250},
    }
    (DATA / "policy_increment_dedup_window.json").write_text(
        json.dumps(dedup_patch, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "primary_records": len(primary),
                "increment_records": len(increment),
                "seed": RANDOM_SEED,
                "data_dir": str(DATA),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
