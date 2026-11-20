from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tcp_sim.api import create_app
from tcp_sim.db import Database
from tcp_sim.service import BatchCommand, SimulationService


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def trace() -> dict[str, Any]:
    return json.loads((ROOT / "samples" / "fixed_trace.json").read_text(encoding="utf-8"))


@pytest.fixture()
def client(tmp_path: Path):
    app = create_app(tmp_path / "api.db")
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def service(tmp_path: Path) -> SimulationService:
    database = Database(tmp_path / "service.db")
    database.initialize()
    return SimulationService(database)


def command(
    trace: dict[str, Any],
    *,
    run_id: str,
    batch_id: str,
    events: list[dict[str, Any]],
    final_batch: bool,
    expected_base: str | None = None,
    include_config: bool = True,
) -> BatchCommand:
    return BatchCommand(
        trace_id=trace["trace_id"],
        run_id=run_id,
        batch_id=batch_id,
        events=events,
        final_batch=final_batch,
        expected_base_checkpoint_id=expected_base,
        config=trace["config"] if include_config else None,
    )


def api_batch(
    trace: dict[str, Any],
    *,
    run_id: str,
    batch_id: str,
    events: list[dict[str, Any]],
    final_batch: bool,
    expected_base: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "trace_id": trace["trace_id"],
        "run_id": run_id,
        "batch_id": batch_id,
        "events": events,
        "final_batch": final_batch,
        "config": trace["config"],
    }
    if expected_base is not None:
        payload["expected_base_checkpoint_id"] = expected_base
    return payload
