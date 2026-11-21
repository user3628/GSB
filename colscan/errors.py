from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        http_status: int = 422,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


def error(
    code: str,
    message: str,
    *,
    http_status: int = 422,
    details: dict[str, Any] | None = None,
) -> BusinessError:
    return BusinessError(code, message, http_status=http_status, details=details)

