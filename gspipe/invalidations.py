"""Persistent, content-addressed invalidation audit events."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .canonical import sha256_json
from .fsutil import atomic_write_json


class InvalidationStore:
    def __init__(self, workspace_root: str | Path) -> None:
        self.root = Path(workspace_root) / ".gspipe" / "state" / "invalidations"

    def record(self, event: Mapping[str, Any]) -> dict[str, Any]:
        payload = dict(event)
        event_id = sha256_json(payload)
        payload["event_id"] = event_id
        path = self.root / str(payload["task_id"]) / f"{event_id}.json"
        if not path.exists():
            atomic_write_json(path, payload)
        return payload

    def load(self, task_id: str) -> list[dict[str, Any]]:
        directory = self.root / task_id
        if not directory.exists():
            return []
        events: list[dict[str, Any]] = []
        for path in sorted(directory.glob("*.json")):
            try:
                event = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(event, dict) and event.get("task_id") == task_id:
                events.append(event)
        events.sort(key=lambda item: (int(item.get("detected_seq", 0)), str(item.get("event_id", ""))))
        return events
