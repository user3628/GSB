from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def new_query_id(prefix: str = "q") -> str:
    return f"{prefix}-{uuid.uuid4().hex}"


def new_round_id() -> str:
    return f"r-{uuid.uuid4().hex}"


@dataclass(slots=True)
class QuerySpec:
    name: str
    qtype: int
    qclass: int = 1
    query_id: str = ""
    priority: int = 0
    hot: bool = False
    round_id: str | None = None
    source_transport: str = "http"
    client: str = "127.0.0.1"
    arrival_seq: int = 0

    def __post_init__(self) -> None:
        if not self.query_id:
            self.query_id = new_query_id()

    @property
    def cache_key(self) -> str:
        return make_cache_key(self.name, self.qtype, self.qclass)

    @property
    def fingerprint(self) -> str:
        text = f"{self.name}|{self.qtype}|{self.qclass}"
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @property
    def priority_score(self) -> int:
        return self.priority + (25 if self.hot else 0)


def make_cache_key(name: str, qtype: int, qclass: int) -> str:
    return f"{name}|{qtype}|{qclass}"


@dataclass(slots=True)
class ItemResult:
    query_id: str
    cache_key: str
    name: str
    qtype: int
    qclass: int
    outcome: str
    rcode: int = 2
    source_type: int | None = None
    business_code: str | None = None
    message: str | None = None
    context: dict[str, Any] = field(default_factory=dict)
    answers: list[dict[str, Any]] = field(default_factory=list)
    authorities: list[dict[str, Any]] = field(default_factory=list)
    ttl_base_ms: int | None = None
    cache: dict[str, Any] | None = None
    wait_ms: int = 0
    merge_decision: dict[str, Any] | None = None

    @property
    def ok(self) -> bool:
        return self.business_code is None and self.rcode in (0, 3)

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "query_id": self.query_id,
            "cache_key": self.cache_key,
            "name": self.name,
            "type": self.qtype,
            "qclass": self.qclass,
            "outcome": self.outcome,
            "rcode": self.rcode,
            "business_code": self.business_code,
            "source_record_type": self.source_type,
            "answers": self.answers,
            "authorities": self.authorities,
            "ttl_base_ms": self.ttl_base_ms,
            "wait_ms": self.wait_ms,
        }
        if self.business_code:
            data["error"] = {
                "code": self.business_code,
                "message": self.message,
                "context": self.context,
            }
        if self.cache is not None:
            data["cache"] = self.cache
        if self.merge_decision is not None:
            data["merge_decision"] = self.merge_decision
        return data


@dataclass(slots=True)
class BatchResult:
    round_id: str
    started_at_ms: int
    finished_at_ms: int
    budget_ms: int
    items: list[ItemResult]
    trace: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "round_id": self.round_id,
            "started_at_ms": self.started_at_ms,
            "finished_at_ms": self.finished_at_ms,
            "elapsed_ms": self.finished_at_ms - self.started_at_ms,
            "budget_ms": self.budget_ms,
            "items": [item.to_dict() for item in self.items],
            "trace": self.trace,
        }

