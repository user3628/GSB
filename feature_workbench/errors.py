from __future__ import annotations

from typing import Any


HTTP_STATUS_BY_CODE = {
    "E_COLUMN_MISMATCH": 422,
    "E_FOLD_INVALID": 422,
    "E_INVALID_THRESHOLD": 422,
    "E_BUDGET_EXCEEDED": 409,
    "E_STALE_CHECKPOINT": 409,
    "E_WORKER_FAILED": 500,
    "E_CONFIG_INVALID": 422,
    "E_INPUT_INVALID": 422,
    "E_STATE_INVALID": 409,
    "E_RUN_IN_PROGRESS": 409,
    "E_NOT_FOUND": 404,
    "E_INTERNAL": 500,
}


class WorkbenchError(Exception):
    """A business error with a stable machine-readable code."""

    def __init__(
        self,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    @property
    def http_status(self) -> int:
        return HTTP_STATUS_BY_CODE.get(self.code, 400)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


def raise_error(
    code: str,
    message: str,
    **details: Any,
) -> None:
    raise WorkbenchError(code, message, details)

