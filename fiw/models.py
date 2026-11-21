"""Normalized in-memory representations of catalog, plan and round results."""

from __future__ import annotations

from dataclasses import dataclass, field

FAULT_CONNECTION_REFUSED = "connection_refused"
FAULT_CONNECTION_TIMEOUT = "connection_timeout"
FAULT_CORRUPT_BODY = "corrupt_body"
FAULT_TYPES = {FAULT_CONNECTION_REFUSED, FAULT_CONNECTION_TIMEOUT, FAULT_CORRUPT_BODY}

PROFILE_HEALTHY = "healthy"
PROFILE_BLACKHOLE = "blackhole"
PROFILE_UNLISTENED = "unlistened"
PROFILES = {PROFILE_HEALTHY, PROFILE_BLACKHOLE, PROFILE_UNLISTENED}

STATUS_PLANNED = "PLANNED"
STATUS_RUNNING = "RUNNING"
STATUS_PAUSED = "PAUSED"
STATUS_COMPLETED = "COMPLETED"
STATUS_ABORTED = "ABORTED"

OUTCOME_INJECTED = "INJECTED"
OUTCOME_TIMEOUT = "TIMEOUT"
OUTCOME_SKIPPED = "SKIPPED"
OUTCOME_UNSUPPORTED = "FAULT_UNSUPPORTED"
OUTCOME_QUEUE_FULL = "QUEUE_FULL"

CONCLUSION_NORMAL = "NORMAL"
CONCLUSION_UNSTABLE = "UNSTABLE"
CONCLUSION_UNREACHABLE = "UNREACHABLE"
CONCLUSION_SEVERITY = {
    CONCLUSION_NORMAL: 0,
    CONCLUSION_UNSTABLE: 1,
    CONCLUSION_UNREACHABLE: 2,
}


@dataclass(frozen=True)
class EndpointSpec:
    endpoint_id: str
    node: str
    weight: float
    upstreams: tuple
    host: str
    port: int
    profile: str


@dataclass(frozen=True)
class InjectionItem:
    item_id: str
    target: str
    fault_type: str
    duration_rounds: int
    priority: int


@dataclass(frozen=True)
class SchedulerConfig:
    global_concurrency: int = 2
    queue_capacity: int = 4
    round_time_budget_ms: float = 2000.0
    connect_timeout_ms: float = 300.0
    cost_estimates_ms: dict = field(default_factory=dict)

    def estimate(self, fault_type):
        return float(self.cost_estimates_ms.get(fault_type, 50.0))


@dataclass(frozen=True)
class PlanSpec:
    plan_id: str
    version: int
    rounds: int
    fault_switches: dict
    scheduler: SchedulerConfig
    items: tuple


@dataclass(frozen=True)
class Catalog:
    endpoints: tuple

    @property
    def by_id(self):
        return {e.endpoint_id: e for e in self.endpoints}
