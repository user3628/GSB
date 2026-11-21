from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any

from .domain import ObservationBatch, PolicySnapshot, ServiceObservation, ServiceSpec
from .errors import observation_invalid
from .units import INT64_MAX, ceil_div


FORMULA_VERSION = "fixed-point-v1"


@dataclass(frozen=True)
class Derivation:
    service_id: str
    formula_version: str
    current_replicas: int
    arrivals: int
    sample_window_ms: int
    arrival_rate_milli_rps: int
    target_concurrency: int
    queue_duration_ms: int
    p99_latency_ms: int
    effective_latency_ms: int
    latency_budget_ms: int
    latency_ratio_milli: int
    latency_breached: bool
    current_floor_replicas: int
    current_floor_applied: bool
    concurrency_replicas: int
    latency_adjusted_replicas: int
    desired_unclamped: int
    desired_replicas: int
    min_replicas: int
    max_replicas: int
    clamp_min_applied: bool
    clamp_max_applied: bool
    calculation: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _multiply_checked(left: int, right: int, label: str) -> int:
    if left < 0 or right < 0:
        raise observation_invalid(f"{label} must be non-negative")
    if left and right > INT64_MAX // left:
        raise observation_invalid(f"{label} overflows signed 64-bit range")
    return left * right


def derive_target(
    service: ServiceSpec,
    observation: ServiceObservation,
    window_ms: int,
) -> Derivation:
    """Compute a deterministic target from integer milli-unit quantities.

    arrival_rate_milli_rps = floor(arrivals * 1_000_000 / window_ms)
    concurrency_replicas = ceil(arrival_rate_milli_rps / (target_concurrency * 1000))
    latency_ratio_milli = floor(max(p99, queue) * 1000 / latency_budget_ms)
    latency_adjusted = ceil(concurrency_replicas * latency_ratio_milli / 1000)
    desired = clamp(max(concurrency_replicas, latency_adjusted), min, max)
    """

    if service.target_concurrency <= 0:
        raise observation_invalid(
            "target_concurrency must be > 0 to compute a target",
            service_id=service.id,
            target_concurrency=service.target_concurrency,
        )
    if service.latency_budget_ms <= 0:
        raise observation_invalid(
            "latency_budget_ms must be > 0 to compute a target",
            service_id=service.id,
            latency_budget_ms=service.latency_budget_ms,
        )
    if window_ms <= 0:
        raise observation_invalid("sample window_ms must be > 0", window_ms=window_ms)

    scaled_arrivals = _multiply_checked(
        observation.arrivals, 1_000_000, "arrivals * 1_000_000"
    )
    arrival_rate_milli = scaled_arrivals // window_ms
    concurrency_denominator = _multiply_checked(
        service.target_concurrency, 1000, "target_concurrency * 1000"
    )
    concurrency_replicas = ceil_div(arrival_rate_milli, concurrency_denominator)

    effective_latency = max(observation.p99_latency_ms, observation.queue_duration_ms)
    latency_numerator = _multiply_checked(
        effective_latency, 1000, "effective_latency * 1000"
    )
    latency_ratio_milli = latency_numerator // service.latency_budget_ms
    latency_adjusted = ceil_div(
        _multiply_checked(
            concurrency_replicas,
            latency_ratio_milli,
            "concurrency_replicas * latency_ratio_milli",
        ),
        1000,
    )
    # While the latency budget is breached, do not scale below the currently
    # committed replica count.  This is deterministic hysteresis, not a random
    # smoothing heuristic, and it explicitly consumes current_replicas.
    current_floor = service.current_replicas if effective_latency > service.latency_budget_ms else 0
    desired_unclamped = max(concurrency_replicas, latency_adjusted, current_floor)
    current_floor_applied = current_floor > max(concurrency_replicas, latency_adjusted)
    clamp_min = desired_unclamped < service.min_replicas
    clamp_max = desired_unclamped > service.max_replicas
    desired = min(max(desired_unclamped, service.min_replicas), service.max_replicas)

    calculation = (
        f"rate={observation.arrivals}*1000000//{window_ms}={arrival_rate_milli}mreq/s; "
        f"concurrency=ceil({arrival_rate_milli}/({service.target_concurrency}*1000))"
        f"={concurrency_replicas}; "
        f"latency_ratio=floor({effective_latency}*1000/{service.latency_budget_ms})"
        f"={latency_ratio_milli}; "
        f"latency_adjusted=ceil({concurrency_replicas}*{latency_ratio_milli}/1000)"
        f"={latency_adjusted}; "
        f"current_floor={current_floor}; "
        f"clamp({desired_unclamped},{service.min_replicas},{service.max_replicas})"
        f"={desired}"
    )

    return Derivation(
        service_id=service.id,
        formula_version=FORMULA_VERSION,
        current_replicas=service.current_replicas,
        arrivals=observation.arrivals,
        sample_window_ms=window_ms,
        arrival_rate_milli_rps=arrival_rate_milli,
        target_concurrency=service.target_concurrency,
        queue_duration_ms=observation.queue_duration_ms,
        p99_latency_ms=observation.p99_latency_ms,
        effective_latency_ms=effective_latency,
        latency_budget_ms=service.latency_budget_ms,
        latency_ratio_milli=latency_ratio_milli,
        latency_breached=effective_latency > service.latency_budget_ms,
        current_floor_replicas=current_floor,
        current_floor_applied=current_floor_applied,
        concurrency_replicas=concurrency_replicas,
        latency_adjusted_replicas=latency_adjusted,
        desired_unclamped=desired_unclamped,
        desired_replicas=desired,
        min_replicas=service.min_replicas,
        max_replicas=service.max_replicas,
        clamp_min_applied=clamp_min,
        clamp_max_applied=clamp_max,
        calculation=calculation,
    )


def derive_batch(
    policy: PolicySnapshot,
    observations: ObservationBatch,
    current_by_service: dict[str, int] | None = None,
) -> tuple[list[Derivation], list[int]]:
    """Derive targets in observation arrival order.

    Returns derivations and the arrival-order index for deterministic scheduler
    tie-breaking. Every policy service must have exactly one observation.
    """

    observed = observations.service_map
    policy_services = policy.service_map
    unknown = sorted(set(observed) - set(policy_services))
    if unknown:
        raise observation_invalid(
            "observation references unknown service(s)", service_ids=unknown
        )
    missing = sorted(set(policy_services) - set(observed))
    if missing:
        raise observation_invalid(
            "observation is missing service(s)", service_ids=missing
        )

    derivations: list[Derivation] = []
    arrival_order: list[int] = []
    for index, observation in enumerate(observations.services):
        service = policy_services[observation.service_id]
        if current_by_service is not None and service.id in current_by_service:
            service = ServiceSpec(
                id=service.id,
                model=service.model,
                current_replicas=current_by_service[service.id],
                min_replicas=service.min_replicas,
                max_replicas=service.max_replicas,
                replica_memory_bytes=service.replica_memory_bytes,
                replica_compute_milli=service.replica_compute_milli,
                target_concurrency=service.target_concurrency,
                latency_budget_ms=service.latency_budget_ms,
                priority=service.priority,
                max_replicas_per_fault_domain=service.max_replicas_per_fault_domain,
            )
        derivations.append(derive_target(service, observation, observations.window_ms))
        arrival_order.append(index)
    return derivations, arrival_order
