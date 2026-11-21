from __future__ import annotations

import pytest

from app.domain import ServiceObservation, ServiceSpec, parse_policy
from app.engine import derive_target
from app.errors import AppError
from app.units import INT64_MAX, parse_compute_milli, parse_memory


def test_parse_quantity_units_and_overflow() -> None:
    assert parse_memory("2Gi") == 2 * 1024**3
    assert parse_memory("512Mi") == 512 * 1024**2
    assert parse_memory("1024") == 1024
    assert parse_compute_milli("250m") == 250
    assert parse_compute_milli("4") == 4000
    assert parse_compute_milli(1500) == 1500

    with pytest.raises(AppError) as negative:
        parse_memory("-1Mi")
    assert negative.value.code == "E_SPEC_INVALID"

    with pytest.raises(AppError) as overflow:
        parse_memory(f"{INT64_MAX}Pi")
    assert overflow.value.code == "E_SPEC_INVALID"

    with pytest.raises(AppError):
        parse_compute_milli("-250m")


def _service(target_concurrency: int = 8, latency_budget_ms: int = 300) -> ServiceSpec:
    return ServiceSpec(
        id="svc",
        model="model",
        current_replicas=1,
        min_replicas=1,
        max_replicas=6,
        replica_memory_bytes=1024,
        replica_compute_milli=500,
        target_concurrency=target_concurrency,
        latency_budget_ms=latency_budget_ms,
        priority=1,
        max_replicas_per_fault_domain=1,
    )


def test_fixed_point_derivation_is_exact_and_repeatable() -> None:
    observation = ServiceObservation(
        service_id="svc",
        sampled_at="2026-11-21T09:00:00+08:00",
        arrivals=480,
        queue_duration_ms=45,
        p99_latency_ms=350,
    )
    first = derive_target(_service(), observation, 60000)
    second = derive_target(_service(), observation, 60000)
    assert first.as_dict() == second.as_dict()
    assert first.arrival_rate_milli_rps == 8000
    assert first.concurrency_replicas == 1
    assert first.latency_ratio_milli == 1166
    assert first.latency_adjusted_replicas == 2
    assert first.desired_replicas == 2
    assert first.calculation == second.calculation


def test_derivation_clamps_and_rejects_uncomputable_targets() -> None:
    observation = ServiceObservation(
        service_id="svc",
        sampled_at="2026-11-21T09:00:00+08:00",
        arrivals=10_000,
        queue_duration_ms=900,
        p99_latency_ms=1200,
    )
    result = derive_target(_service(), observation, 60000)
    assert result.desired_unclamped > 6
    assert result.desired_replicas == 6
    assert result.clamp_max_applied is True
    assert result.clamp_min_applied is False

    missing = ServiceObservation(
        service_id="svc",
        sampled_at="2026-11-21T09:00:00+08:00",
        arrivals=0,
        queue_duration_ms=0,
        p99_latency_ms=0,
    )
    with pytest.raises(AppError) as error:
        derive_target(_service(target_concurrency=0), missing, 60000)
    assert error.value.code == "E_OBSERVATION_INVALID"


def test_target_zero_is_structurally_accepted_but_not_computable() -> None:
    raw = {
        "version": 1,
        "generated_at": "2026-11-21T09:00:00+08:00",
        "runtime": {
            "max_parallel_scales": 1,
            "round_time_budget_ms": 10,
            "placement_cost_ms": 10,
            "wait_queue_limit": 1,
        },
        "services": [
            {
                "id": "svc",
                "model": "m",
                "current_replicas": 0,
                "min_replicas": 0,
                "max_replicas": 1,
                "replica_memory": "1Mi",
                "replica_compute": "10m",
                "target_concurrency": 0,
                "latency_budget_ms": 10,
                "priority": 1,
                "max_replicas_per_fault_domain": 1,
            }
        ],
        "nodes": [
            {
                "id": "n1",
                "fault_domain": "d1",
                "memory": "1Gi",
                "compute_share": "1",
            }
        ],
    }
    policy = parse_policy(raw)
    assert policy.services[0].target_concurrency == 0


def test_current_replicas_are_a_floor_while_latency_is_breached() -> None:
    observation = ServiceObservation(
        service_id="svc",
        sampled_at="2026-11-21T09:00:00+08:00",
        arrivals=60,
        queue_duration_ms=10,
        p99_latency_ms=400,
    )
    service = _service(target_concurrency=8, latency_budget_ms=200)
    service = ServiceSpec(
        id=service.id,
        model=service.model,
        current_replicas=4,
        min_replicas=service.min_replicas,
        max_replicas=service.max_replicas,
        replica_memory_bytes=service.replica_memory_bytes,
        replica_compute_milli=service.replica_compute_milli,
        target_concurrency=service.target_concurrency,
        latency_budget_ms=service.latency_budget_ms,
        priority=service.priority,
        max_replicas_per_fault_domain=service.max_replicas_per_fault_domain,
    )
    result = derive_target(service, observation, 60000)
    assert result.latency_breached is True
    assert result.current_floor_replicas == 4
    assert result.current_floor_applied is True
    assert result.desired_replicas == 4
