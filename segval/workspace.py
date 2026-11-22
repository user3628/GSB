"""Local workspace lifecycle, stop control, and state persistence."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .canonical import FileLock, atomic_write_json, locked, read_json
from .config import DEFAULT_CONFIG, deep_merge, save_config, validate_config
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Workspace:
    def __init__(self, root: Path) -> None:
        self.root = Path(root).resolve()
        self.config_path = self.root / "config.json"
        self.state_path = self.root / "state.json"
        self.stop_path = self.root / "STOP.json"
        self.run_lock_path = self.root / ".run.lock"
        self.progress_lock_path = self.root / ".progress.lock"
        self.config_lock_path = self.root / ".config.lock"
        self.shards_dir = self.root / "shards"
        self.checkpoint_dir = self.root / "checkpoints"
        self.exports_dir = self.root / "exports"
        self.tmp_dir = self.root / "tmp"

    def initialize(self, config: dict[str, Any] | None = None, *, reset_config: bool = False) -> "Workspace":
        for directory in (
            self.root,
            self.shards_dir,
            self.checkpoint_dir,
            self.exports_dir,
            self.tmp_dir,
        ):
            directory.mkdir(parents=True, exist_ok=True)
        if reset_config or not self.config_path.exists():
            save_config(self.config_path, validate_config(config or DEFAULT_CONFIG))
        if not self.state_path.exists():
            atomic_write_json(
                self.state_path,
                {
                    "schema_version": 1,
                    "status": "new",
                    "stop_requested": False,
                    "stopped": False,
                    "last_run": None,
                },
            )
        return self

    def config(self) -> dict[str, Any]:
        return validate_config(read_json(self.config_path))

    def update_config(self, patch: dict[str, Any]) -> dict[str, Any]:
        with locked(self.config_lock_path):
            current = self.config() if self.config_path.exists() else DEFAULT_CONFIG
            merged = deep_merge(current, patch)
            return save_config(self.config_path, merged)

    def state(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {"schema_version": 1, "status": "new", "last_run": None}
        return read_json(self.state_path)

    def save_state(self, state: dict[str, Any]) -> None:
        atomic_write_json(self.state_path, state)

    def run_lock(self) -> FileLock:
        return FileLock(self.run_lock_path, blocking=False)

    def request_stop(self, reason: str = "user_request") -> dict[str, Any]:
        payload = {"requested": True, "reason": reason, "requested_at": utc_now()}
        atomic_write_json(self.stop_path, payload)
        return payload

    def stop_requested(self) -> bool:
        return self.stop_path.exists()

    def stop_info(self) -> dict[str, Any] | None:
        if not self.stop_path.exists():
            return None
        try:
            return read_json(self.stop_path)
        except Exception:
            return {"requested": True, "reason": "unreadable_stop_file"}

    def clear_stop(self) -> None:
        if self.stop_path.exists():
            try:
                self.stop_path.unlink()
            except FileNotFoundError:
                pass

    def new_run_token(self) -> str:
        return uuid.uuid4().hex

    def progress_path(self, run_token: str) -> Path:
        return self.tmp_dir / f"progress-{run_token}.json"

    def record_sample_boundary(self, run_token: str, stop_after_samples: int | None) -> int:
        if stop_after_samples is None:
            return self._progress_count(run_token)
        with locked(self.progress_lock_path):
            path = self.progress_path(run_token)
            if path.exists():
                progress = read_json(path)
            else:
                progress = {"run_token": run_token, "completed_samples": 0}
            progress["completed_samples"] = int(progress.get("completed_samples", 0)) + 1
            progress["stop_after_samples"] = int(stop_after_samples)
            atomic_write_json(path, progress)
            if int(progress["completed_samples"]) >= int(stop_after_samples):
                if not self.stop_path.exists():
                    self.request_stop("stop_after_samples_reached")
            return int(progress["completed_samples"])

    def _progress_count(self, run_token: str) -> int:
        path = self.progress_path(run_token)
        if not path.exists():
            return 0
        try:
            return int(read_json(path).get("completed_samples", 0))
        except Exception:
            return 0

    def cleanup_progress(self, run_token: str) -> None:
        path = self.progress_path(run_token)
        if path.exists():
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    def acquire_publish_lock(self) -> FileLock:
        return FileLock(self.root / ".publish.lock", blocking=True)


def create_workspace(root: Path, config: dict[str, Any] | None = None) -> Workspace:
    return Workspace(root).initialize(config=config)

