from __future__ import annotations

from typing import Any


ERROR_STATUS = {
    "E_INVALID_REQUEST": 400,
    "E_NOT_FOUND": 404,
    "E_STALE_SNAPSHOT": 409,
    "E_STALE_VERSION": 409,
    "E_STOPPING": 409,
    "E_TIMELINE_ERROR": 422,
    "E_INCOMPAT_FORMAT": 422,
    "E_ROUNDTRIP_MISMATCH": 500,
    "E_LOCK_TIMEOUT": 503,
    "E_IO": 500,
    "E_INTERNAL": 500,
}


class WorkbenchError(Exception):
    """A stable, machine-readable business error."""

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
        self.http_status = ERROR_STATUS.get(code, 500)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


def timeline_error(
    *,
    chunk_id: str,
    entry_index: int | None,
    entry_id: str | None,
    violation: str,
    message: str,
    **extra: Any,
) -> WorkbenchError:
    details: dict[str, Any] = {
        "chunk_id": chunk_id,
        "entry_index": entry_index,
        "entry_id": entry_id,
        "violation": violation,
    }
    details.update(extra)
    return WorkbenchError("E_TIMELINE_ERROR", message, details)


def incompat_format(message: str, **details: Any) -> WorkbenchError:
    return WorkbenchError("E_INCOMPAT_FORMAT", message, details)


def stale_snapshot(message: str, **details: Any) -> WorkbenchError:
    return WorkbenchError("E_STALE_SNAPSHOT", message, details)


def stale_version(message: str, **details: Any) -> WorkbenchError:
    return WorkbenchError("E_STALE_VERSION", message, details)
