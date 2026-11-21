from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    return default if raw is None else int(raw)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class GatewayConfig:
    """Runtime limits. Route-level limits may lower these caps, never raise them."""

    db_path: str = "gateway.db"
    global_concurrency_limit: int = 4
    queue_limit: int = 128
    round_time_budget_ms: int = 500
    default_request_timeout_ms: int = 5_000
    recovery_requeue: bool = True
    max_recovery_attempts: int = 3
    validate_loopback_endpoints: bool = True

    @classmethod
    def from_env(cls) -> "GatewayConfig":
        return cls(
            db_path=os.getenv("GATEWAY_DB_PATH", "gateway.db"),
            global_concurrency_limit=_env_int("GATEWAY_GLOBAL_CONCURRENCY", 4),
            queue_limit=_env_int("GATEWAY_QUEUE_LIMIT", 128),
            round_time_budget_ms=_env_int("GATEWAY_ROUND_BUDGET_MS", 500),
            default_request_timeout_ms=_env_int(
                "GATEWAY_DEFAULT_REQUEST_TIMEOUT_MS", 5_000
            ),
            recovery_requeue=_env_bool("GATEWAY_RECOVERY_REQUEUE", True),
            max_recovery_attempts=_env_int("GATEWAY_MAX_RECOVERY_ATTEMPTS", 3),
            validate_loopback_endpoints=_env_bool(
                "GATEWAY_VALIDATE_LOOPBACK_ENDPOINTS", True
            ),
        )

    def validate(self) -> None:
        if self.global_concurrency_limit < 1:
            raise ValueError("global_concurrency_limit must be >= 1")
        if self.queue_limit < 0:
            raise ValueError("queue_limit must be >= 0")
        if self.round_time_budget_ms < 1:
            raise ValueError("round_time_budget_ms must be >= 1")
        if self.default_request_timeout_ms < 1:
            raise ValueError("default_request_timeout_ms must be >= 1")
        if self.max_recovery_attempts < 0:
            raise ValueError("max_recovery_attempts must be >= 0")

    @property
    def resolved_db_path(self) -> str:
        return str(Path(self.db_path).expanduser().resolve())

