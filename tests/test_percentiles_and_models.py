from __future__ import annotations

import pytest

from capacity_workbench.errors import InsufficientSamples, PlanInvalid
from capacity_workbench.models import PlanCreate, validate_plan_payload
from capacity_workbench.percentiles import compute_percentiles, nearest_rank


def test_nearest_rank_is_deterministic() -> None:
    values = list(range(1, 101))
    assert nearest_rank(values, 0.50) == 50
    assert nearest_rank(values, 0.95) == 95
    assert nearest_rank(values, 0.99) == 99
    assert compute_percentiles(values) == {
        "p50_ms": 50.0,
        "p95_ms": 95.0,
        "p99_ms": 99.0,
    }


def test_p99_requires_one_hundred_samples() -> None:
    with pytest.raises(InsufficientSamples) as error:
        compute_percentiles(list(range(99)))
    assert error.value.code == "E_INSUFFICIENT_SAMPLES"
    assert error.value.details == {
        "sample_size": 99,
        "required": 100,
        "percentile": "P99",
    }


@pytest.mark.parametrize(
    "mutation",
    [
        lambda p: p["stages"].reverse(),
        lambda p: p["stages"][0].update(concurrency=0),
        lambda p: p["stages"][0].update(duration_seconds=0),
        lambda p: p["thresholds"].update(p99_ms=0),
        lambda p: p["thresholds"].update(error_rate=1.2),
        lambda p: p["thresholds"].update(min_throughput_rps=-1),
        lambda p: p.update(target_url="http://example.com:80/sample"),
        lambda p: p.update(request_timeout_ms=0),
    ],
)
def test_plan_validation_rejects_invalid_semantics(base_plan: dict, mutation) -> None:
    payload = {
        **base_plan,
        "stages": [dict(stage) for stage in base_plan["stages"]],
        "thresholds": dict(base_plan["thresholds"]),
    }
    mutation(payload)
    with pytest.raises(PlanInvalid) as error:
        validate_plan_payload(payload)
    assert error.value.code == "E_PLAN_INVALID"


def test_plan_document_adds_stable_id(base_plan: dict) -> None:
    document = PlanCreate.model_validate({**base_plan, "id": "plan_fixed"}).document()
    assert document["id"] == "plan_fixed"
    assert document["stages"][1]["concurrency"] == 2

