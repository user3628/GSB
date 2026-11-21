from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from replica_repair.http_client import LocalHttpClient  # noqa: E402
from replica_repair.numerics import canonical_json  # noqa: E402
from replica_repair.service import RepairService  # noqa: E402


FIXED_DEADLINE = "2000-01-01T00:00:05Z"


def load_json(relative_path: str) -> Any:
    with (ROOT / relative_path).open("r", encoding="utf-8") as handle:
        return json.load(handle)


def group_payload() -> dict[str, Any]:
    return copy.deepcopy(load_json("samples/groups.json")["groups"][0])


def report_payload(
    name: str,
    *,
    round_id: str,
    **overrides: Any,
) -> dict[str, Any]:
    report = copy.deepcopy(load_json(f"samples/reports/{name}"))
    report["report_id"] = f"{round_id}:{report['report_id']}"
    report.update(overrides)
    return report


def round_payload(
    round_id: str,
    *,
    deadline: str = FIXED_DEADLINE,
    target_progress: Any = 0.25,
    tolerance: Any = 1e-8,
) -> dict[str, Any]:
    return {
        "round_id": round_id,
        "group_id": "grp-fixed",
        "group_version": 1,
        "deadline": deadline,
        "target_progress": target_progress,
        "relative_tolerance": tolerance,
    }


def make_service(
    temporary_directory: tempfile.TemporaryDirectory[str] | str | Path,
    *,
    now_fn: Any = None,
    sleep_fn: Any = None,
    aggregation_delay_ms: int = 0,
) -> RepairService:
    if isinstance(temporary_directory, (str, Path)):
        directory = Path(temporary_directory)
    else:
        directory = Path(temporary_directory.name)
    db_path = directory / "test.sqlite3"
    kwargs: dict[str, Any] = {
        "aggregation_delay_ms": aggregation_delay_ms,
        "auto_recover": True,
    }
    if now_fn is None:
        fixed = datetime(2000, 1, 1, 0, 0, 3, tzinfo=timezone.utc)
        kwargs["now_fn"] = lambda: fixed
    else:
        kwargs["now_fn"] = now_fn
    if sleep_fn is not None:
        kwargs["sleep_fn"] = sleep_fn
    return RepairService(db_path, **kwargs)


class FakeClock:
    def __init__(self, value: datetime | None = None) -> None:
        self.value = value or datetime(2026, 1, 1, tzinfo=timezone.utc)
        self.sleeps: list[float] = []

    def now(self) -> datetime:
        return self.value

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.value = self.value + timedelta(seconds=seconds)


class RunningServer:
    def __init__(self, *, port: int = 18765) -> None:
        self.port = port
        self.tempdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tempdir.name) / "http.sqlite3"
        self.log_path = Path(self.tempdir.name) / "server.log"
        self.process: subprocess.Popen[bytes] | None = None
        self.log_handle: Any = None
        self.client = LocalHttpClient(f"http://127.0.0.1:{port}")

    def __enter__(self) -> "RunningServer":
        env = os.environ.copy()
        env.pop("REPAIR_AGGREGATION_DELAY_MS", None)
        env["REPAIR_CLOCK_BASE_ISO"] = "2000-01-01T00:00:03Z"
        self.log_handle = self.log_path.open("ab")
        self.process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "replica_repair.server",
                "--db",
                str(self.db_path),
                "--host",
                "127.0.0.1",
                "--port",
                str(self.port),
            ],
            cwd=ROOT,
            env=env,
            stdout=self.log_handle,
            stderr=subprocess.STDOUT,
        )
        self.client.wait_until_healthy(timeout_seconds=8.0)
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=3)
        if self.log_handle is not None:
            self.log_handle.close()
        self.tempdir.cleanup()

    def failure_log(self) -> str:
        if not self.log_path.exists():
            return ""
        return self.log_path.read_text(encoding="utf-8", errors="replace")


def expect_http_success(
    result: tuple[int, dict[str, Any]],
    *,
    context: str,
) -> dict[str, Any]:
    status, payload = result
    if status not in (200, 202) or not payload.get("ok"):
        raise AssertionError(f"{context}: HTTP {status} {canonical_json(payload)}")
    return payload

