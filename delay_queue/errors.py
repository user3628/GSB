from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    http_status: int = 400

    def __post_init__(self) -> None:
        super().__init__(f"{self.code}: {self.message}")

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


E_TASK_INVALID = "E_TASK_INVALID"
E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
E_QUEUE_FULL = "E_QUEUE_FULL"
E_DISPATCH_TIMEOUT = "E_DISPATCH_TIMEOUT"
E_CONFLICTING_ROUND = "E_CONFLICTING_ROUND"
E_IDEMPOTENCY_CONFLICT = "E_IDEMPOTENCY_CONFLICT"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_NOT_FOUND = "E_NOT_FOUND"
E_ROUND_IN_PROGRESS = "E_ROUND_IN_PROGRESS"
E_PROCESS_INTERRUPTED = "E_PROCESS_INTERRUPTED"
E_DISPATCH_CONNECTION = "E_DISPATCH_CONNECTION"
E_DISPATCH_HTTP_ERROR = "E_DISPATCH_HTTP_ERROR"
E_INTERNAL = "E_INTERNAL"

