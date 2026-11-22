from __future__ import annotations

from typing import Any


class EngineError(Exception):
    """A deterministic business error returned by the engine and HTTP layer."""

    _STATUS = {
        "E_MEASURE_TYPE_MISMATCH": 422,
        "E_INVALID_TRANSITION": 409,
        "E_LATE_EVENT": 409,
        "E_SNAPSHOT_INVALID": 422,
        "E_CONFLICTING_BATCH": 409,
        "E_BATCH_ABORTED": 409,
        "E_BATCH_IN_PROGRESS": 409,
        "E_OUT_OF_ORDER": 422,
        "E_DUPLICATE_EVENT": 422,
        "E_INVALID_WINDOW_SPEC": 422,
        "E_INVALID_REQUEST": 422,
        "E_NOT_FOUND": 404,
        "E_INTERNAL": 500,
    }

    def __init__(
        self,
        code: str,
        message: str,
        context: dict[str, Any] | None = None,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context or {}
        self.status_code = status_code or self._STATUS.get(code, 400)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


def invalid(code: str, message: str, **context: Any) -> EngineError:
    return EngineError(code, message, context)
