from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class PushState(str, Enum):
    PENDING = "PENDING"
    PUSHED = "PUSHED"
    ACKED = "ACKED"
    APPLIED = "APPLIED"
    ABORTED = "ABORTED"


ALLOWED_TRANSITIONS: dict[PushState, frozenset[PushState]] = {
    PushState.PENDING: frozenset({PushState.PUSHED, PushState.ABORTED}),
    PushState.PUSHED: frozenset({PushState.ACKED, PushState.ABORTED}),
    PushState.ACKED: frozenset({PushState.APPLIED, PushState.ABORTED}),
    PushState.APPLIED: frozenset(),
    PushState.ABORTED: frozenset(),
}


@dataclass(frozen=True)
class SampleSpec:
    seed: int = 20240501
    sample_size: int = 64
    feature_count: int = 3

    def as_dict(self) -> dict[str, int]:
        return {
            "seed": self.seed,
            "sample_size": self.sample_size,
            "feature_count": self.feature_count,
        }


@dataclass(frozen=True)
class Snapshot:
    version: int
    generated_at: str
    config: dict[str, float]
    digest: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "generated_at": self.generated_at,
            "config": dict(self.config),
            "digest": self.digest,
        }


@dataclass(frozen=True)
class ClientRegistration:
    client_id: str
    config_keys: tuple[str, ...]
    endpoint_url: str
    timeout_seconds: float = 1.0
    sample: SampleSpec = field(default_factory=SampleSpec)

    def as_dict(self) -> dict[str, Any]:
        return {
            "client_id": self.client_id,
            "config_keys": list(self.config_keys),
            "endpoint_url": self.endpoint_url,
            "timeout_seconds": self.timeout_seconds,
            "sample": self.sample.as_dict(),
        }

