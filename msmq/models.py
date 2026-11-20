from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

Semantics = Literal["at_least_once", "exactly_once"]
MessageState = Literal["pending", "leased", "acked", "expired"]

DELIVERY_STATE_PRIORITY: dict[str, int] = {
    "expired": 1,
    "pending": 2,
    "leased": 3,
    "acked": 4,
}


@dataclass
class QueueMeta:
    name: str
    semantics: Semantics
    visibility_timeout_ms: int
    version: int = 1
    extensions: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "semantics": self.semantics,
            "visibility_timeout_ms": self.visibility_timeout_ms,
            "version": self.version,
            "extensions": self.extensions,
        }


@dataclass
class MessageRecord:
    message_id: str
    queue: str
    body: str
    produced_at_us: int
    expires_at_us: int
    content_type: str = "text/plain"
    state: MessageState = "pending"
    record_version: int = 1
    last_write_at_us: int | None = None
    delivery_count: int = 0
    extensions: dict[str, Any] = field(default_factory=dict)
    lease_owner: str | None = None
    lease_token: str | None = None
    lease_expires_at_us: int | None = None
    source_format: str = "api"
    source_version: int = 1

    def __post_init__(self) -> None:
        if self.last_write_at_us is None:
            self.last_write_at_us = self.produced_at_us

    @property
    def body_bytes(self) -> bytes:
        return self.body.encode("utf-8")

    def semantic_dict(self, *, include_state: bool = True) -> dict[str, Any]:
        value: dict[str, Any] = {
            "message_id": self.message_id,
            "queue": self.queue,
            "body": self.body,
            "content_type": self.content_type,
            "produced_at_us": self.produced_at_us,
            "expires_at_us": self.expires_at_us,
            "record_version": self.record_version,
            "last_write_at_us": self.last_write_at_us,
            "extensions": self.extensions,
        }
        if include_state:
            value.update(
                {
                    "state": self.state,
                    "delivery_count": self.delivery_count,
                    "lease_owner": self.lease_owner,
                    "lease_token": self.lease_token,
                    "lease_expires_at_us": self.lease_expires_at_us,
                }
            )
        return value


@dataclass
class ParsedBatch:
    format: str
    version: int
    decision: str
    queues: list[QueueMeta]
    messages: list[MessageRecord]
    decode_report: dict[str, Any] = field(default_factory=dict)
    raw_size: int = 0
    sha256: str = ""


def message_payload_equal(left: MessageRecord, right: MessageRecord) -> bool:
    return left.semantic_dict() == right.semantic_dict()
