from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from gitops_convergence.engine import Engine, EngineConfig  # noqa: E402


def app(
    app_id: str,
    *,
    cluster_id: str = "c1",
    desired: str = "v1",
    effective: str = "v1",
    replicas: int = 1,
    dependencies: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "app_id": app_id,
        "cluster_id": cluster_id,
        "desired_version": desired,
        "effective_version": effective,
        "replica_budget": replicas,
        "dependencies": dependencies or [],
    }


def snapshot(
    applications: list[dict[str, Any]],
    *,
    version: int = 1,
    generated_at: str = "2026-01-01T00:00:00Z",
    clusters: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "version": version,
        "generated_at": generated_at,
        "clusters": clusters or [{"cluster_id": "c1", "replica_quota": 100}],
        "applications": applications,
    }


def increment(
    *,
    version: int,
    base_version: int,
    changes: list[dict[str, Any]],
    generated_at: str = "2026-01-01T00:01:00Z",
) -> dict[str, Any]:
    return {
        "version": version,
        "base_version": base_version,
        "generated_at": generated_at,
        "changes": changes,
    }


@pytest.fixture()
def engine(tmp_path: Path) -> Engine:
    return Engine(
        tmp_path / "state.db",
        EngineConfig(switch_threshold=1000, max_batch_replicas=100),
    )


def load_sample(name: str) -> dict[str, Any]:
    return json.loads((ROOT / "samples" / name).read_text(encoding="utf-8"))
