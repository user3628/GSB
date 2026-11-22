"""Atomic non-deterministic runtime metrics kept outside content fingerprints."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .fsutil import atomic_write_json


class MetricsStore:
    def __init__(self, workspace_root: str | Path) -> None:
        self.root = Path(workspace_root) / ".gspipe" / "metrics"

    def write_execution(self, task_id: str, artifact_digest: str, metrics: dict[str, Any]) -> None:
        path = self.root / "executions" / task_id / f"{artifact_digest}.json"
        atomic_write_json(path, metrics)

    def read_execution(self, task_id: str, artifact_digest: str) -> dict[str, Any] | None:
        path = self.root / "executions" / task_id / f"{artifact_digest}.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    def write_last_run(self, report: dict[str, Any]) -> None:
        atomic_write_json(self.root / "last_run.json", report)

    def read_last_run(self) -> dict[str, Any] | None:
        path = self.root / "last_run.json"
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None
