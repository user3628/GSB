"""Regenerate the deterministic local samples (run from any directory)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app.policy import SCHEMA_V1, SCHEMA_V2, compute_checksum  # noqa: E402

T0 = 1_767_225_600_000  # 2026-01-01T00:00:00Z
T1 = 1_798_761_600_000  # 2027-01-01T00:00:00Z
GI = 1_073_741_824
MI = 1_048_576


def policy(
    tenant_id: str,
    parent_id: str | None,
    cpu: int,
    memory: int,
    burst_cpu: int,
    burst_memory: int,
    rate: int,
    capacity: int,
    initial: int,
) -> dict:
    return {
        "tenant_id": tenant_id,
        "parent_id": parent_id,
        "quota": {
            "cpu_millicores": cpu,
            "memory_bytes": memory,
            "burst": {
                "cpu_millicores": burst_cpu,
                "memory_bytes": burst_memory,
            },
        },
        "rate_limit": {
            "refill_per_second": rate,
            "capacity": capacity,
            "initial_tokens": initial,
        },
        "lifecycle": {"start_ms": T0, "end_ms": T1},
    }


def hierarchical() -> dict:
    return {
        "schema_version": SCHEMA_V2,
        "version": 2,
        "generated_at_ms": T0,
        "tenants": [
            policy("platform", None, 10_000, 32 * GI, 2_000, 8 * GI, 100, 200, 200),
            policy("team-alpha", "platform", 6_000, 12 * GI, 1_000, 2 * GI, 40, 80, 80),
            policy("team-beta", "platform", 1_500, 4 * GI, 300, 1 * GI, 20, 40, 40),
            policy("alice", "team-alpha", 4_000, 10 * GI, 500, 2 * GI, 20, 40, 40),
            policy("bob", "team-beta", 4_000, 4 * GI, 500, 1 * GI, 10, 20, 20),
            policy("dave", "team-beta", 4_000, 4 * GI, 500, 1 * GI, 10, 20, 20),
            policy("edge", "team-alpha", 1_000, 1 * GI, 0, 0, 1, 2, 2),
            policy("starter", "platform", 500, 1 * GI, 0, 0, 2, 2, 0),
        ],
    }


def legacy_tenant(
    tenant_id: str,
    parent_id: str | None,
    cpu: int,
    memory: int,
    rate: int,
    capacity: int,
    initial: int,
    burst_cpu: int,
    burst_memory: int,
    **extra,
) -> dict:
    item = {
        "tenant_id": tenant_id,
        "cpu_millicores_limit": cpu,
        "memory_bytes_limit": memory,
        "requests_per_second_limit": rate,
        "deprecated_burst_tokens": capacity,
        "deprecated_initial_tokens": initial,
        "deprecated_burst_cpu_millicores": burst_cpu,
        "deprecated_burst_memory_bytes": burst_memory,
        "lifecycle": {"start_ms": T0, "end_ms": T1},
    }
    if parent_id is not None:
        item["deprecated_parent_tenant_id"] = parent_id
    item.update(extra)
    return item


def legacy() -> dict:
    return {
        "schema_version": SCHEMA_V1,
        "version": 1,
        "generated_at_ms": T0,
        "tenants": [
            legacy_tenant("platform", None, 10_000, 24 * GI, 100, 120, 120, 0, 0),
            legacy_tenant("team-alpha", "platform", 5_000, 10 * GI, 30, 40, 40, 500, 512 * MI),
            legacy_tenant("team-beta", "platform", 1_000, 3 * GI, 10, 15, 15, 200, 256 * MI),
            legacy_tenant("alice", "team-alpha", 3_000, 8 * GI, 15, 20, 20, 500, 256 * MI),
            legacy_tenant("bob", "team-beta", 2_500, 3 * GI, 8, 10, 10, 300, 128 * MI),
            legacy_tenant("dave", "team-beta", 2_500, 3 * GI, 8, 10, 10, 300, 128 * MI),
            legacy_tenant("edge", "team-alpha", 1_000, 1 * GI, 1, 1, 1, 0, 0),
            legacy_tenant("starter", "platform", 500, 1 * GI, 2, 2, 0, 0, 0),
        ],
    }


def batch() -> dict:
    return {
        "batch_id": "batch-2026-01-01-a",
        "applications": [
            {
                "request_id": "req-starter-1",
                "tenant_id": "starter",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 500,
                "cpu_millicores": 100,
                "memory_bytes": 2 * MI,
            },
            {
                "request_id": "req-starter-2",
                "tenant_id": "starter",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 1_000,
                "cpu_millicores": 100,
                "memory_bytes": 2 * MI,
            },
            {
                "request_id": "req-alice-ok-1",
                "tenant_id": "alice",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 1_000,
                "cpu_millicores": 1_200,
                "memory_bytes": 1 * GI,
            },
            {
                "request_id": "req-alice-ok-2",
                "tenant_id": "alice",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 1_100,
                "cpu_millicores": 1_800,
                "memory_bytes": 2 * GI,
            },
            {
                "request_id": "req-starter-3",
                "tenant_id": "starter",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 1_200,
                "cpu_millicores": 100,
                "memory_bytes": 2 * MI,
            },
            {
                "request_id": "req-alice-quota",
                "tenant_id": "alice",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 1_200,
                "cpu_millicores": 1_600,
                "memory_bytes": 1 * GI,
            },
            {
                "request_id": "req-bob-ok",
                "tenant_id": "bob",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 1_300,
                "cpu_millicores": 1_000,
                "memory_bytes": 512 * MI,
            },
            {
                "request_id": "req-dave-parent",
                "tenant_id": "dave",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 1_400,
                "cpu_millicores": 1_000,
                "memory_bytes": 512 * MI,
            },
            {
                "request_id": "req-edge-1",
                "tenant_id": "edge",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 1_500,
                "cpu_millicores": 100,
                "memory_bytes": 1 * MI,
            },
            {
                "request_id": "req-edge-2",
                "tenant_id": "edge",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 1_600,
                "cpu_millicores": 100,
                "memory_bytes": 1 * MI,
            },
            {
                "request_id": "req-edge-3",
                "tenant_id": "edge",
                "namespace": "shared-prod",
                "arrival_ms": T0 + 1_700,
                "cpu_millicores": 100,
                "memory_bytes": 1 * MI,
            },
        ],
    }


def conflict_migration() -> dict:
    document = {
        "schema_version": SCHEMA_V1,
        "version": 1,
        "generated_at_ms": T0,
        "tenants": [
            legacy_tenant(
                "mig-clean",
                "platform",
                800,
                2 * GI,
                5,
                10,
                10,
                100,
                128 * MI,
            ),
            legacy_tenant(
                "mig-conflict",
                "platform",
                900,
                2 * GI,
                5,
                40,
                10,
                100,
                128 * MI,
                legacy_burst_tokens=80,
                deprecated_qos_class="premium",
            ),
        ],
    }
    return document


def write(name: str, value: dict, *, checksum: bool = True) -> None:
    if checksum:
        value = dict(value)
        value.pop("checksum", None)
        value["checksum"] = compute_checksum(value)
    path = ROOT / "samples" / name
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def main() -> None:
    write("policies_hierarchical_v2.json", hierarchical())
    write("policies_flat_v1.json", legacy())
    write("requests_batch.json", batch(), checksum=False)
    write("migration_conflict.json", conflict_migration())


if __name__ == "__main__":
    main()
