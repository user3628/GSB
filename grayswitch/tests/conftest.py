"""pytest 公共夹具：临时数据库 + FastAPI TestClient + 确定性样本。"""

from __future__ import annotations

import json
import os
import sys

import pytest
from fastapi.testclient import TestClient

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from app.api import create_app  # noqa: E402
from app.models import canonical_digest  # noqa: E402


def _sample(name: str):
    with open(os.path.join(ROOT, "samples", name), encoding="utf-8") as fh:
        return json.load(fh)


@pytest.fixture()
def client(tmp_path):
    db = tmp_path / "test.db"
    app = create_app(str(db))
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def samples():
    return {
        "policy_v1": _sample("policy_v1.json"),
        "policy_v2": _sample("policy_v2.json"),
        "policy_v3": _sample("policy_v3.json"),
        "batch1": _sample("events_batch_1.json"),
        "batch2": _sample("events_batch_2.json"),
        "batch3": _sample("events_batch_3.json"),
        "mixed": _sample("mixed_magnitude.json"),
        "bad_policy": _sample("bad_policy_digest.json"),
        "bad_events": _sample("bad_events.json"),
        "zero_policy": _sample("zero_weight_policy.json"),
    }


@pytest.fixture()
def make_policy():
    def _make(version=1, weights=None, mirror_rate="0.25", gray_cap="0.35",
              rel_tol="0.000001", window_ms=10000, baseline="stable",
              seed="test-seed", generated_at_ms=1000):
        payload = {
            "version": version,
            "generated_at_ms": generated_at_ms,
            "version_weights": weights or {"stable": "0.70", "canary": "0.30"},
            "mirror_rate": mirror_rate,
            "gray_cap": gray_cap,
            "relative_tolerance": rel_tol,
            "dedup_window_ms": window_ms,
            "baseline_version": baseline,
            "seed": seed,
        }
        payload["digest"] = canonical_digest(payload)
        return payload

    return _make


def submit_policy(client, payload):
    return client.post("/policies", json=payload)


def submit_batch(client, payload):
    return client.post("/events", json=payload)
