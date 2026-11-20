"""Configuration and path handling for the engine."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


@dataclass(frozen=True, slots=True)
class Settings:
    root: Path
    data_dir: Path
    db_path: Path
    intent_log_path: Path
    recovery_report_path: Path
    shutdown_report_path: Path
    runtime_path: Path
    seed_path: Path
    host: str = "127.0.0.1"
    port: int = 0
    max_active_txns: int = 8
    max_waiters: int = 4
    lock_wait_ms: int = 600
    edit_budget_ms: int = 2500
    busy_timeout_ms: int = 30000

    def with_overrides(self, **values: Any) -> "Settings":
        clean = {key: value for key, value in values.items() if value is not None}
        return replace(self, **clean)

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.runtime_path.parent.mkdir(parents=True, exist_ok=True)


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def load_settings(
    root: Path | str | None = None,
    params_path: Path | str | None = None,
    data_dir: Path | str | None = None,
    db_path: Path | str | None = None,
    host: str | None = None,
    port: int | None = None,
) -> Settings:
    root_path = Path(root).resolve() if root else project_root()
    params_file = Path(params_path).resolve() if params_path else root_path / "samples" / "demo_params.json"
    params: dict[str, Any] = {}
    if params_file.exists():
        params = json.loads(params_file.read_text(encoding="utf-8"))

    data_path = Path(data_dir).resolve() if data_dir else root_path / "data"
    db_file = Path(db_path).resolve() if db_path else data_path / "mvcc.sqlite3"
    seed_file = root_path / "samples" / "seed.json"

    settings = Settings(
        root=root_path,
        data_dir=data_path,
        db_path=db_file,
        intent_log_path=data_path / "intent-log.jsonl",
        recovery_report_path=data_path / "recovery-report.json",
        shutdown_report_path=data_path / "shutdown-report.json",
        runtime_path=data_path / "server-runtime.json",
        seed_path=seed_file,
        host=str(host or params.get("host", "127.0.0.1")),
        port=int(port if port is not None else params.get("port", 0)),
        max_active_txns=int(params.get("max_active_txns", 8)),
        max_waiters=int(params.get("max_waiters", 4)),
        lock_wait_ms=int(params.get("lock_wait_ms", 600)),
        edit_budget_ms=int(params.get("edit_budget_ms", 2500)),
        busy_timeout_ms=int(params.get("busy_timeout_ms", 30000)),
    )
    settings.ensure_directories()
    return settings


def optional_bool(value: Any) -> bool:
    return _as_bool(value)

