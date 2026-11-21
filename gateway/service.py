from __future__ import annotations

import threading
from typing import Any

from .config import GatewayConfig
from .scheduler import Scheduler
from .store import Store


class GatewayService(Scheduler):
    def __init__(self, config: GatewayConfig | None = None) -> None:
        self.config = config or GatewayConfig.from_env()
        self.config.validate()
        self.store = Store(self.config)
        super().__init__(self.store, self.config)
        self._startup_lock = threading.Lock()
        self._started = False
        self.recovery_report: dict[str, Any] | None = None

    def startup(self) -> dict[str, Any]:
        with self._startup_lock:
            if not self._started:
                self.recovery_report = self.store.recover()
                self._started = True
            return self.recovery_report or {}

    def shutdown(self) -> None:
        self.close()
        self.store.close()

