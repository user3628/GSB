"""Business errors shared by codecs, storage, service and HTTP layers."""

from __future__ import annotations

from typing import Any


HTTP_STATUS_BY_CODE: dict[str, int] = {
    "E_RECORD_CORRUPT": 422,
    "E_FORMAT_UNSUPPORTED": 415,
    "E_COUNT_INVALID": 422,
    "E_INSUFFICIENT_SAMPLES": 422,
    "E_SNAPSHOT_INVALID": 409,
    "E_IMPORT_CONFLICT": 409,
    "E_INCONSISTENT_READINGS": 409,
    "E_CONFIG_INVALID": 400,
    "E_REQUEST_INVALID": 400,
    "E_NOT_FOUND": 404,
    "E_IO": 500,
    "E_INTERNAL": 500,
}


class SloError(Exception):
    """An error with a stable machine-readable business code.

    ``offset`` is a zero-based byte offset for format errors.  It is deliberately
    part of the top-level error object so callers do not have to inspect free-form
    messages.
    """

    def __init__(
        self,
        code: str,
        message: str,
        *,
        offset: int | None = None,
        details: dict[str, Any] | None = None,
        path: str | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.offset = offset
        self.details = details or {}
        self.path = path
        self.http_status = http_status or HTTP_STATUS_BY_CODE.get(code, 500)

    def to_dict(self) -> dict[str, Any]:
        error: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.offset is not None:
            error["offset"] = self.offset
        if self.path is not None:
            error["path"] = self.path
        if self.details:
            error["details"] = self.details
        return {"ok": False, "error": error}


def corrupt(
    path: str,
    message: str,
    offset: int,
    *,
    details: dict[str, Any] | None = None,
) -> SloError:
    return SloError(
        "E_RECORD_CORRUPT",
        message,
        path=path,
        offset=offset,
        details=details,
    )
