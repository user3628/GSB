from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


@dataclass(frozen=True)
class Settings:
    db_path: Path = ROOT / "data" / "runtime.sqlite3"
    log_path: Path = ROOT / "data" / "runtime.log"
    endpoint_base: str = "http://127.0.0.1:8765"
    start_timeout_s: float = 1.0
    exec_timeout_s: float = 2.0
    default_queue_capacity: int = 8
    max_queue_wait_s: float = 20.0
    http_connect_timeout_s: float = 1.0
    sqlite_busy_timeout_s: float = 10.0

    @classmethod
    def from_env(cls) -> "Settings":
        base = cls()
        endpoint = os.getenv("SIM_ENDPOINT_URL", base.endpoint_base).rstrip("/")
        db_raw = os.getenv("SIM_DB_PATH")
        log_raw = os.getenv("SIM_LOG_PATH")
        return replace(
            base,
            db_path=Path(db_raw).resolve() if db_raw else base.db_path,
            log_path=Path(log_raw).resolve() if log_raw else base.log_path,
            endpoint_base=endpoint,
            start_timeout_s=_env_int("SIM_START_TIMEOUT_MS", int(base.start_timeout_s * 1000)) / 1000,
            exec_timeout_s=_env_int("SIM_EXEC_TIMEOUT_MS", int(base.exec_timeout_s * 1000)) / 1000,
            default_queue_capacity=_env_int(
                "SIM_DEFAULT_QUEUE_CAPACITY", base.default_queue_capacity
            ),
            max_queue_wait_s=_env_int(
                "SIM_MAX_QUEUE_WAIT_MS", int(base.max_queue_wait_s * 1000)
            )
            / 1000,
            http_connect_timeout_s=_env_int(
                "SIM_CONNECT_TIMEOUT_MS", int(base.http_connect_timeout_s * 1000)
            )
            / 1000,
        )

