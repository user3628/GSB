from __future__ import annotations

import os
import time
from pathlib import Path


def crash_hook(point: str) -> None:
    """Deterministic test hook for abrupt-process recovery experiments.

    When ``MPC_CRASH_POINT`` matches, an optional marker is written and the
    process holds a live SQLite transaction until the parent kills it.  Keeping
    this outside the recovery paths makes a normal request immune to the hook.
    """

    if os.environ.get("MPC_CRASH_POINT") != point:
        return
    marker = os.environ.get("MPC_CRASH_MARKER")
    if marker:
        marker_path = Path(marker)
        marker_path.parent.mkdir(parents=True, exist_ok=True)
        marker_path.write_text(
            f"pid={os.getpid()} point={point} monotonic={time.monotonic()}\n",
            encoding="utf-8",
        )
    hold_seconds = float(os.environ.get("MPC_CRASH_HOLD_SECONDS", "30"))
    if hold_seconds > 0:
        time.sleep(hold_seconds)
    os._exit(97)
