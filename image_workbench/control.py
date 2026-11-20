from __future__ import annotations

import threading
from pathlib import Path

from .atomic import atomic_write_text


class StopController:
    """A thread-safe and cross-process stop flag.

    The in-memory event supports HTTP callers in the same process; the flag file
    lets a separate CLI invocation request a graceful stop of a server-run job.
    """

    def __init__(self, flag_path: str | Path | None = None) -> None:
        self.flag_path = Path(flag_path) if flag_path is not None else None
        self._event = threading.Event()
        self._lock = threading.Lock()
        if self.flag_path is not None and self.flag_path.exists():
            self._event.set()

    def request_stop(self) -> None:
        with self._lock:
            self._event.set()
            if self.flag_path is not None:
                atomic_write_text(self.flag_path, "stop\n")

    def resume(self) -> None:
        with self._lock:
            self._event.clear()
            if self.flag_path is not None:
                self.flag_path.unlink(missing_ok=True)

    def is_requested(self) -> bool:
        return self._event.is_set() or (self.flag_path is not None and self.flag_path.exists())

    def snapshot(self) -> dict[str, bool]:
        return {"stop_requested": self.is_requested()}

