"""Domain records shared by codecs, normalization, storage and statistics."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


UINT32_MAX = 2**32 - 1
UINT64_MAX = 2**64 - 1

Protocol = Literal["tcp", "udp", "icmp"]
FailureType = Literal["none", "refused", "timeout", "reset", "error"]
TimestampPolicy = Literal["truncate", "trunc", "floor", "ceil", "half_up", "round"]

PROTOCOLS = {"tcp", "udp", "icmp"}
FAILURE_TYPES = {"none", "refused", "timeout", "reset", "error"}
TIMESTAMP_POLICIES = {"truncate", "trunc", "floor", "ceil", "half_up", "round"}


@dataclass(slots=True, frozen=True)
class RawRecord:
    """A record as read from a source format, before unit/time normalization."""

    timestamp_value: int
    timestamp_unit: Literal["ms", "us"]
    probe_id: str
    path_node: str
    protocol: str
    success: bool
    rtt_value: int | None
    rtt_unit: str
    payload_value: int | None
    payload_unit: str
    failure_type: str = "none"


@dataclass(slots=True, frozen=True)
class DiagnosticRecord:
    """Normalized internal record. All RTT values are microseconds and payloads bytes."""

    timestamp_ms: int
    probe_id: str
    path_node: str
    protocol: str
    success: bool
    rtt_us: int
    payload_bytes: int
    failure_type: str = "none"
    ordinal: int = 0
    source_format: str = "internal"
    source_version: int = 2
    rounding_bit: int = 0
    rounding_remainder_us: int = 0
    rounding_error_us: int = 0
    rtt_missing: bool = False
    payload_missing: bool = False

    def core_dict(self) -> dict[str, Any]:
        """The eight field internal identity used for round-trip comparison."""

        return {
            "timestamp_ms": self.timestamp_ms,
            "probe_id": self.probe_id,
            "path_node": self.path_node,
            "protocol": self.protocol,
            "success": self.success,
            "rtt_us": self.rtt_us,
            "payload_bytes": self.payload_bytes,
            "failure_type": self.failure_type,
        }

    def audit_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True, frozen=True)
class NegotiationReport:
    format: str
    version: int
    decision: str
    decision_label: str
    records_detected: int
    units: dict[str, str]
    degradations: tuple[dict[str, Any], ...] = ()
    warnings: tuple[str, ...] = ()
    matrix_version: str = "2026-11-23"

    def as_dict(self) -> dict[str, Any]:
        return {
            "format": self.format,
            "version": self.version,
            "decision": self.decision,
            "decision_label": self.decision_label,
            "records_detected": self.records_detected,
            "units": dict(self.units),
            "degradations": list(self.degradations),
            "warnings": list(self.warnings),
            "matrix_version": self.matrix_version,
        }


@dataclass(slots=True)
class DecodedBundle:
    report: NegotiationReport
    records: list[RawRecord]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class NormalizationResult:
    records: list[DiagnosticRecord]
    report: dict[str, Any]


@dataclass(slots=True, frozen=True)
class ProbeRequest:
    host: str
    port: int
    path_node: str
    probe_id: str
    timeout_ms: int = 750
    payload_bytes: int = 32
    protocol: str = "tcp"


@dataclass(slots=True, frozen=True)
class ProbeObservation:
    record: DiagnosticRecord
    outcome: str
    code: str | None
    message: str
    socket_closed: bool
    elapsed_us: int
    http_status: int = 200

    def as_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome,
            "code": self.code,
            "message": self.message,
            "socket_closed": self.socket_closed,
            "elapsed_us": self.elapsed_us,
            "record": self.record.core_dict(),
        }
