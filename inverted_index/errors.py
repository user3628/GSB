from __future__ import annotations

from typing import Any


class BusinessError(Exception):
    """An error that is safe to expose through the HTTP API."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "error": {
                "code": self.code,
                "message": self.message,
            }
        }
        if self.details:
            body["error"]["details"] = self.details
        return body


def bad_path(path: str, reason: str) -> BusinessError:
    return BusinessError(
        "E_BAD_PATH",
        f"Invalid field path: {path!r}",
        details={"path": path, "reason": reason},
    )


def unknown_path(path: str) -> BusinessError:
    return BusinessError(
        "E_UNKNOWN_PATH",
        f"Field path is not registered for indexing: {path!r}",
        details={"path": path},
    )
