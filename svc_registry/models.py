from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Literal


PROBE_METHODS = ("http", "tcp", "lease")
WEIGHT_ENCODINGS = ("ratio", "permille")
TIME_UNITS = ("ms", "us")
SNAPSHOT_KINDS = ("registration", "probe_result")


@dataclass(frozen=True)
class RegistrationRecord:
    """A record as represented by an input format, before normalization."""

    service_name: str
    instance_id: str
    endpoint: str
    weight: int | float
    weight_encoding: Literal["ratio", "permille"]
    lease_ttl_ms: int
    probe_method: str
    probe_params: dict[str, Any] | None
    registered_at: int
    time_unit: Literal["ms", "us"]
    lease_version: int | None = None
    probe_window_ms: int | None = None
    last_heartbeat_at: int | None = None
    source_version: int = 2


@dataclass(frozen=True)
class ProbeResultRecord:
    """A probe-result snapshot row before normalization."""

    service_name: str
    instance_id: str
    endpoint: str
    weight: int | float
    weight_encoding: Literal["ratio", "permille"]
    probe_method: str
    probe_params: dict[str, Any] | None
    checked_at: int
    time_unit: Literal["ms", "us"]
    success: bool
    status: str
    latency_ms: int
    outcome: str
    lease_version: int | None = None
    probe_window_ms: int | None = None
    error_code: str | None = None
    detail: str | None = None
    source_version: int = 2


@dataclass(frozen=True)
class NormalizedRegistration:
    service_name: str
    instance_id: str
    endpoint: str
    weight_permille: int
    lease_ttl_ms: int
    probe_method: str
    probe_params: dict[str, Any]
    registered_at_ms: int
    lease_version: int = 1
    probe_window_ms: int | None = None
    last_heartbeat_at_ms: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @property
    def key(self) -> tuple[str, str]:
        return (self.service_name, self.instance_id)


@dataclass(frozen=True)
class NormalizedProbeResult:
    service_name: str
    instance_id: str
    endpoint: str
    weight_permille: int
    probe_method: str
    probe_params: dict[str, Any]
    checked_at_ms: int
    success: bool
    status: str
    latency_ms: int
    outcome: str
    lease_version: int | None = None
    probe_window_ms: int | None = None
    error_code: str | None = None
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def canonical_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))


@dataclass
class NormalizationStep:
    record_index: int
    service_name: str
    instance_id: str
    source_weight: int | float
    source_weight_encoding: str
    weight_permille: int
    weight_rounding_rule: str
    weight_rounding_residue: float
    time_unit: str
    truncation_rule: str
    time_residue_us: int
    degradations: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class Snapshot:
    kind: Literal["registration", "probe_result"]
    version: int
    records: tuple[RegistrationRecord | ProbeResultRecord, ...]
    endianness: str | None = None
    source_format: str = "unknown"

    def __len__(self) -> int:
        return len(self.records)
