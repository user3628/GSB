from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from app.engine import schedule_batch
from app.models import BatchSpecInput, normalize_spec

ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


def load_raw(name: str) -> dict[str, Any]:
    with (SAMPLES / f"{name}.json").open("r", encoding="utf-8") as handle:
        return json.load(handle)


def run_sample(name: str, batch_id: str | None = None) -> dict[str, Any]:
    raw = load_raw(name)
    normalized = normalize_spec(BatchSpecInput.model_validate(raw))
    return schedule_batch(normalized, batch_id=batch_id or f"test-{name}")


@pytest.fixture()
def original_result() -> dict[str, Any]:
    return run_sample("original")


@pytest.fixture()
def modified_result() -> dict[str, Any]:
    return run_sample("modified")


@pytest.fixture()
def preemption_result() -> dict[str, Any]:
    return run_sample("preemption")
