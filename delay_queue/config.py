from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


@dataclass(frozen=True)
class Settings:
    db_path: Path = Path("data/delay_queue.db")
    intent_log_dir: Path = Path("intent_logs")
    default_endpoint: str = "http://127.0.0.1:8765/dispatch"
    global_concurrency: int = 64
    default_max_concurrency: int = 4
    default_time_budget_ms: int = 2_000
    default_max_task_resource: int = 100
    default_waiting_queue_capacity: int = 100
    default_dispatch_timeout_ms: int = 1_000
    retry_backoff_base_ms: int = 200
    allow_non_local_endpoints: bool = False
    round_wait_timeout_ms: int = 10_000

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=Path(os.getenv("DQT_DB_PATH", "data/delay_queue.db")),
            intent_log_dir=Path(os.getenv("DQT_INTENT_DIR", "intent_logs")),
            default_endpoint=os.getenv(
                "DQT_DEFAULT_ENDPOINT", "http://127.0.0.1:8765/dispatch"
            ),
            global_concurrency=_env_int("DQT_GLOBAL_CONCURRENCY", 64),
            default_max_concurrency=_env_int("DQT_MAX_CONCURRENCY", 4),
            default_time_budget_ms=_env_int("DQT_TIME_BUDGET_MS", 2_000),
            default_max_task_resource=_env_int("DQT_MAX_TASK_RESOURCE", 100),
            default_waiting_queue_capacity=_env_int(
                "DQT_WAITING_QUEUE_CAPACITY", 100
            ),
            default_dispatch_timeout_ms=_env_int(
                "DQT_DISPATCH_TIMEOUT_MS", 1_000
            ),
            retry_backoff_base_ms=_env_int("DQT_RETRY_BASE_MS", 200),
            allow_non_local_endpoints=os.getenv(
                "DQT_ALLOW_NON_LOCAL_ENDPOINTS", "0"
            ).lower()
            in {"1", "true", "yes"},
            round_wait_timeout_ms=_env_int("DQT_ROUND_WAIT_TIMEOUT_MS", 10_000),
        )

    def validate(self) -> None:
        if self.global_concurrency < 1:
            raise ValueError("global_concurrency must be >= 1")
        if self.default_max_concurrency < 1:
            raise ValueError("default_max_concurrency must be >= 1")
        if self.default_time_budget_ms < 0:
            raise ValueError("default_time_budget_ms must be >= 0")
        if self.default_max_task_resource < 1:
            raise ValueError("default_max_task_resource must be >= 1")
        if self.default_waiting_queue_capacity < 0:
            raise ValueError("default_waiting_queue_capacity must be >= 0")
        if self.default_dispatch_timeout_ms < 1:
            raise ValueError("default_dispatch_timeout_ms must be >= 1")
        if self.retry_backoff_base_ms < 1:
            raise ValueError("retry_backoff_base_ms must be >= 1")

