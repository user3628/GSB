"""Stable business errors and exit-code mapping."""

from __future__ import annotations

from typing import Any, Mapping


class WorkbenchError(Exception):
    """An expected, stable business error with machine-readable context."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        context: Mapping[str, Any] | None = None,
        http_status: int = 400,
        exit_code: int = 2,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = dict(context or {})
        self.http_status = http_status
        self.exit_code = exit_code

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "context": self.context,
            },
        }


E_INVALID_REGION = "E_INVALID_REGION"
E_NUMERIC = "E_NUMERIC"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_MANIFEST = "E_MANIFEST"
E_CONFIG = "E_CONFIG"
E_NOT_FOUND = "E_NOT_FOUND"
E_BUSY = "E_BUSY"
E_IO = "E_IO"


EXIT_CODES = {
    E_INVALID_REGION: 10,
    E_NUMERIC: 11,
    E_STALE_CHECKPOINT: 12,
    E_INVALID_STAGE: 13,
    E_MANIFEST: 14,
    E_CONFIG: 15,
    E_NOT_FOUND: 20,
    E_BUSY: 21,
    E_IO: 22,
}


def raise_error(
    code: str,
    message: str,
    *,
    context: Mapping[str, Any] | None = None,
    http_status: int | None = None,
) -> None:
    if http_status is None:
        http_status = 404 if code == E_NOT_FOUND else 409 if code in {E_STALE_CHECKPOINT, E_BUSY} else 400
        if code == E_NUMERIC:
            http_status = 422
    raise WorkbenchError(
        code,
        message,
        context=context,
        http_status=http_status,
        exit_code=EXIT_CODES.get(code, 1),
    )
