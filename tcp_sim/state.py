from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from .config import SimulatorConfig


CLOSED = "CLOSED"
SYN_SENT = "SYN_SENT"
ESTABLISHED = "ESTABLISHED"
FIN_WAIT = "FIN_WAIT"
TIME_WAIT = "TIME_WAIT"


COUNTER_KEYS = (
    "send_events",
    "ack_events",
    "loss_events",
    "timeout_events",
    "dup_ack_events",
    "new_acks",
    "duplicate_acks",
    "out_of_order_acks",
    "slow_start_acks",
    "congestion_avoidance_acks",
    "fast_retransmits",
    "fast_recovery_inflates",
    "timeout_growth_suppressed",
    "rtt_updates",
)


@dataclass
class Segment:
    start_seq: int
    end_seq: int
    sent_at_us: int
    retransmitted: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ConnectionState:
    conn_id: str
    state: str = CLOSED
    cwnd_bytes: int = 0
    ssthresh_bytes: int = 0
    srtt_us: int = 0
    rttvar_us: int = 0
    rto_us: int = 0
    rto_backoff: int = 1
    highest_ack_seq: int = 0
    highest_sent_seq: int = 0
    dup_ack_count: int = 0
    fast_recovery: bool = False
    growth_blocked_until_us: int = 0
    last_timeout_at_us: int | None = None
    last_send_at_us: int | None = None
    last_send_rtt_valid: bool = False
    outstanding: list[Segment] = field(default_factory=list)
    counters: dict[str, int] = field(default_factory=lambda: {key: 0 for key in COUNTER_KEYS})
    last_event_index: int = -1
    last_event_ts_ms: int = -1

    @classmethod
    def fresh(cls, conn_id: str, config: SimulatorConfig) -> "ConnectionState":
        return cls(
            conn_id=conn_id,
            state=CLOSED,
            cwnd_bytes=config.initial_cwnd_bytes,
            ssthresh_bytes=config.initial_ssthresh_bytes,
            rto_us=config.min_rto_us,
        )

    def public_dict(self, config: SimulatorConfig) -> dict[str, Any]:
        return {
            "conn_id": self.conn_id,
            "state": self.state,
            "cwnd_bytes": self.cwnd_bytes,
            "ssthresh_bytes": self.ssthresh_bytes,
            "srtt_us": self.srtt_us,
            "rttvar_us": self.rttvar_us,
            "rto_us": self.rto_us,
            "rto_backoff": self.rto_backoff,
            "min_cwnd_bytes": config.min_cwnd_bytes,
            "initial_cwnd_bytes": config.initial_cwnd_bytes,
            "mss_bytes": config.mss_bytes,
            "highest_ack_seq": self.highest_ack_seq,
            "highest_sent_seq": self.highest_sent_seq,
            "dup_ack_count": self.dup_ack_count,
            "fast_recovery": self.fast_recovery,
            "growth_blocked_until_us": self.growth_blocked_until_us,
            "last_timeout_at_us": self.last_timeout_at_us,
            "last_send_at_us": self.last_send_at_us,
            "outstanding": [segment.to_dict() for segment in self.outstanding],
            "counters": dict(self.counters),
            "last_event_index": self.last_event_index,
            "last_event_ts_ms": self.last_event_ts_ms,
        }

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        return value

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ConnectionState":
        data = dict(value)
        data["outstanding"] = [Segment(**segment) for segment in data.get("outstanding", [])]
        counters = {key: 0 for key in COUNTER_KEYS}
        counters.update({key: int(val) for key, val in data.get("counters", {}).items()})
        data["counters"] = counters
        return cls(**data)


@dataclass
class SimulationState:
    schema_version: int
    config: dict[str, Any]
    connections: dict[str, ConnectionState] = field(default_factory=dict)
    last_event_index: int = -1
    last_event_ts_ms: int = -1
    committed_event_count: int = 0

    @classmethod
    def empty(cls, config: SimulatorConfig) -> "SimulationState":
        return cls(schema_version=1, config=config.to_dict())

    def state_config(self) -> SimulatorConfig:
        return SimulatorConfig.from_dict(self.config)

    def connection(self, conn_id: str, config: SimulatorConfig) -> ConnectionState:
        if conn_id not in self.connections:
            self.connections[conn_id] = ConnectionState.fresh(conn_id, config)
        return self.connections[conn_id]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "config": dict(self.config),
            "connections": {
                conn_id: self.connections[conn_id].to_dict()
                for conn_id in sorted(self.connections)
            },
            "last_event_index": self.last_event_index,
            "last_event_ts_ms": self.last_event_ts_ms,
            "committed_event_count": self.committed_event_count,
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "SimulationState":
        connections = {
            conn_id: ConnectionState.from_dict(conn_value)
            for conn_id, conn_value in value.get("connections", {}).items()
        }
        return cls(
            schema_version=int(value.get("schema_version", 1)),
            config=dict(value["config"]),
            connections=connections,
            last_event_index=int(value.get("last_event_index", -1)),
            last_event_ts_ms=int(value.get("last_event_ts_ms", -1)),
            committed_event_count=int(value.get("committed_event_count", 0)),
        )
