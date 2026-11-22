from __future__ import annotations

from typing import Any


class WorkbenchError(Exception):
    """A failure that is safe to expose as a stable business error."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        context: dict[str, Any] | None = None,
        status_code: int = 400,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context or {}
        self.status_code = status_code

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "context": self.context,
            },
        }


def fail(
    code: str,
    message: str,
    *,
    context: dict[str, Any] | None = None,
    status_code: int = 400,
) -> None:
    raise WorkbenchError(code, message, context=context, status_code=status_code)

