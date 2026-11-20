from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from capacity_ledger.checksum import compute_payload_checksum  # noqa: E402
from capacity_ledger.service import CapacityLedger  # noqa: E402


@pytest.fixture
def sample_root() -> Path:
    return ROOT / "samples"


@pytest.fixture
def ledger(tmp_path: Path) -> CapacityLedger:
    return CapacityLedger(tmp_path / "ledger.sqlite3", tmp_path / "intents")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sign(payload: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(payload)
    result.pop("checksum", None)
    result["checksum"] = compute_payload_checksum(result)
    return result


def clone_with_commit_id(payload: dict[str, Any], commit_id: str) -> dict[str, Any]:
    result = copy.deepcopy(payload)
    result["commit_id"] = commit_id
    return sign(result)


def snapshot_from_services(
    services: dict[str, dict[str, int]],
    *,
    version: int = 1,
    commit_id: str = "test-snapshot",
    total_budget: int = 10_000,
    tolerance_ppm: int = 1000,
) -> dict[str, Any]:
    payload = {
        "kind": "snapshot",
        "commit_id": commit_id,
        "version": version,
        "generated_at": "2026-01-15T08:00:00Z",
        "total_budget_cents_per_hour": total_budget,
        "tolerance_ppm": tolerance_ppm,
        "services": copy.deepcopy(services),
    }
    return sign(payload)


def increment_from_snapshot(
    snapshot: dict[str, Any],
    *,
    service_name: str,
    changes: dict[str, int],
    commit_id: str = "test-increment",
) -> dict[str, Any]:
    service = copy.deepcopy(snapshot["services"][service_name])
    service.update(changes)
    payload = {
        "kind": "increment",
        "commit_id": commit_id,
        "base_version": snapshot["version"],
        "version": snapshot["version"] + 1,
        "generated_at": "2026-01-15T09:00:00Z",
        "services": {service_name: service},
    }
    return sign(payload)


def final_v3_services(sample_root: Path) -> dict[str, dict[str, int]]:
    snapshot = load_json(sample_root / "snapshot_v1.json")
    services = copy.deepcopy(snapshot["services"])
    for filename in ("01_auth_price_increment.json", "02_catalog_cores_increment.json"):
        increment = load_json(sample_root / "increments" / filename)
        for name, service in increment["services"].items():
            services[name] = service
    return services

