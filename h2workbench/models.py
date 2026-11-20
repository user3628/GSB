from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class FrameType(str, Enum):
    HEADERS = "HEADERS"
    DATA = "DATA"
    WINDOW_UPDATE = "WINDOW_UPDATE"
    RST_STREAM = "RST_STREAM"
    SETTINGS = "SETTINGS"


class StreamState(str, Enum):
    IDLE = "IDLE"
    OPEN = "OPEN"
    HALF_CLOSED_LOCAL = "HALF_CLOSED_LOCAL"
    HALF_CLOSED_REMOTE = "HALF_CLOSED_REMOTE"
    CLOSED = "CLOSED"
    RESET = "RESET"


class JobStatus(str, Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    BLOCKED = "BLOCKED"
    CANCELLED = "CANCELLED"


class RunStatus(str, Enum):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    BLOCKED = "BLOCKED"
    ABORTED = "ABORTED"
    LIMIT_REACHED = "LIMIT_REACHED"


ACTIVE_STATES = {
    StreamState.OPEN,
    StreamState.HALF_CLOSED_LOCAL,
    StreamState.HALF_CLOSED_REMOTE,
}

SENDABLE_LOCAL_STATES = {
    StreamState.OPEN,
    StreamState.HALF_CLOSED_REMOTE,
}

MAX_WINDOW = (1 << 31) - 1
MAX_WEIGHT = 256
MAX_PRIORITY = 255


@dataclass(frozen=True)
class FrameSpec:
    index: int
    connection_id: str
    stream_id: int
    frame_type: FrameType
    flags: frozenset[str]
    payload_bytes: int = 0
    window_increment: int = 0
    direction: str = "local"
    weight: int | None = None
    priority: int | None = None


@dataclass(frozen=True)
class ConnectionConfig:
    connection_window: int = 65_535
    stream_window: int = 65_535
    round_budget: int = 16_384
    candidate_threshold: int = 256
    candidate_limit: int = 64
    priority_scale: int = 4
    max_rounds: int = 100_000

    def to_dict(self) -> dict[str, int]:
        return {
            "connection_window": self.connection_window,
            "stream_window": self.stream_window,
            "round_budget": self.round_budget,
            "candidate_threshold": self.candidate_threshold,
            "candidate_limit": self.candidate_limit,
            "priority_scale": self.priority_scale,
            "max_rounds": self.max_rounds,
        }


@dataclass(frozen=True)
class SampleSpec:
    sample_id: str
    connection_id: str
    frames: tuple[FrameSpec, ...]
    batch_size: int
    config: ConnectionConfig | None
    content_hash: str
    canonical_json: str


@dataclass
class SimStream:
    stream_id: int
    state: StreamState
    initial_window: int
    window: int
    sent_unacked: int
    pending_bytes: int
    weight: int
    priority: int
    sent_total: int = 0
    pending_end_stream: bool = False
    created: bool = False


@dataclass
class SimConnection:
    connection_id: str
    initial_connection_window: int
    connection_window: int
    initial_stream_window: int
    closed: bool
    config: ConnectionConfig
    protocol_violations: int = 0


@dataclass
class SimJob:
    job_key: int
    sample_id: str
    frame_index: int
    stream_id: int
    remaining_bytes: int
    original_bytes: int
    end_stream: bool
    status: str = JobStatus.PENDING.value


@dataclass
class Transition:
    stream_id: int
    from_state: StreamState
    to_state: StreamState
    trigger: str
    frame_index: int | None = None
    round_index: int | None = None


@dataclass
class Violation:
    stream_id: int
    code: str
    requested_increment: int
    applied_increment: int
    current_before: int
    initial_limit: int
    frame_index: int
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class RoundStreamPlan:
    stream_id: int
    bytes: int
    priority: int
    weight: int
    reason: str
    job_consumption: list[tuple[int, int]] = field(default_factory=list)
    end_stream: bool = False
    state_before: StreamState = StreamState.OPEN
    state_after: StreamState = StreamState.OPEN


@dataclass
class BlockedPlan:
    stream_id: int
    required_bytes: int
    pending_bytes: int
    connection_window: int
    stream_window: int
    code: str = "E_FLOW_CONTROL_BLOCKED"
    reason: str = "connection or stream flow-control window is insufficient"


@dataclass
class RoundPlan:
    round_index: int
    algorithm: str
    candidate_count: int
    selected_candidate_count: int
    truncated_count: int
    approximation: dict[str, Any]
    selections: list[RoundStreamPlan]
    blocked: list[BlockedPlan]
    connection_window_before: int
    connection_window_after: int
    selected_bytes: int = 0
    final_status: str | None = None


@dataclass
class SchedulePlan:
    run_id: str
    connection_id: str
    sample_id: str | None
    rounds: list[RoundPlan]
    final_status: str
    error_context: dict[str, Any] | None = None
    note: str | None = None
