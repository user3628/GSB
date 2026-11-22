from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """An error that is safe to expose as a stable business error code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        context: dict[str, Any] | None = None,
        status_code: int = 409,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context or {}
        self.status_code = status_code

    def as_dict(self, state_epoch: int | None = None) -> dict[str, Any]:
        error: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }
        if state_epoch is not None:
            error["state_epoch"] = state_epoch
        return {"error": error}


class SimulatedForcedTermination(Exception):
    """Raised by the in-process crash injection mode.

    The in-process mode is reported as a simulation error; the hard-exit mode is
    the one that actually terminates the process.
    """

    def __init__(self, point: str, batch_id: str | None = None) -> None:
        self.point = point
        self.batch_id = batch_id
        super().__init__(f"simulated forced termination at {point}")
