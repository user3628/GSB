from __future__ import annotations

import os
from pathlib import Path

from .api import create_runtime_app
from .config import PROJECT_ROOT, PlannerConfig


def _env_path(name: str, default: Path) -> Path:
    value = os.environ.get(name)
    return Path(value).expanduser().resolve() if value else default


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    return int(value) if value is not None else default


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    return float(value) if value is not None else default


config = PlannerConfig(
    max_relations=_env_int("PLANNER_MAX_RELATIONS", 8),
    fast_candidate_budget=_env_int("PLANNER_FAST_BUDGET", 4),
    build_delay_seconds=_env_float("PLANNER_BUILD_DELAY_SECONDS", 0.0),
)

app = create_runtime_app(
    snapshot_dir=_env_path("PLANNER_SNAPSHOT_DIR", PROJECT_ROOT / "data" / "snapshots"),
    query_catalog_path=_env_path(
        "PLANNER_QUERY_CATALOG", PROJECT_ROOT / "data" / "queries" / "queries.json"
    ),
    runtime_dir=_env_path("PLANNER_RUNTIME_DIR", PROJECT_ROOT / "runtime"),
    initial_snapshot_filename=os.environ.get("PLANNER_INITIAL_SNAPSHOT", "snapshot_v1.json"),
    config=config,
)

