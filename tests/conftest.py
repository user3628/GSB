from __future__ import annotations

import json
from pathlib import Path
from typing import Iterator

import pytest

from replication_guard.models import Policy, normalize_policy
from replication_guard.service import ReplicationService


ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


@pytest.fixture()
def sample_policy_raw() -> list[dict]:
    return json.loads((SAMPLES / "policies.json").read_text("utf-8"))


@pytest.fixture()
def sample_policies(sample_policy_raw: list[dict]) -> list[Policy]:
    return [normalize_policy(raw) for raw in sample_policy_raw]


@pytest.fixture()
def sample_observations() -> dict:
    return json.loads((SAMPLES / "observations.json").read_text("utf-8"))


@pytest.fixture()
def service(tmp_path: Path) -> Iterator[ReplicationService]:
    instance = ReplicationService(tmp_path / "test.sqlite3")
    instance.startup()
    yield instance

