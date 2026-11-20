from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """An expected, API-safe failure carrying a stable business code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 422,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        error: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details:
            error["details"] = self.details
        return {"error": error}


def invalid_quantity(message: str, *, path: str, value: Any) -> BusinessError:
    return BusinessError(
        "E_INVALID_QUANTITY",
        message,
        details={"path": path, "value": value},
    )

