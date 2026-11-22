from __future__ import annotations

from typing import Any


HTTP_STATUS = {
    "E_BAD_REQUEST": 400,
    "E_KEY_NOT_FOUND": 404,
    "E_NOT_FOUND": 404,
    "E_KEY_EXPIRED": 404,
    "E_CONFLICTING_BATCH": 409,
    "E_IMPORT_REJECTED": 409,
    "E_SNAPSHOT_STALE": 409,
    "E_STALE_BASE": 409,
    "E_VERSION_CONFLICT": 409,
    "E_FORMAT_MISMATCH": 415,
    "E_FORMAT_UNSUPPORTED": 415,
    "E_RECORD_CORRUPT": 422,
    "E_SNAPSHOT_INVALID": 422,
    "E_SEGMENT_INVALID": 422,
    "E_INTERNAL": 500,
}


class StoreError(Exception):
    """A stable, machine-readable business error.

    Every error carries a business code and a JSON-compatible context.  HTTP
    handlers deliberately do not expose Python tracebacks to clients.
    """

    def __init__(
        self,
        code: str,
        message: str,
        context: dict[str, Any] | None = None,
        *,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = context or {}
        self.http_status = http_status or HTTP_STATUS.get(code, 400)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


def record_corrupt(
    message: str,
    *,
    record_index: int,
    format_name: str,
    **context: Any,
) -> StoreError:
    ctx: dict[str, Any] = {
        "format": format_name,
        "record_index": record_index,
        **context,
    }
    if "record_position" not in ctx:
        if ctx.get("line_no") is not None:
            ctx["record_position"] = f"line {ctx['line_no']} (record #{record_index})"
        elif ctx.get("byte_offset") is not None:
            ctx["record_position"] = (
                f"record #{record_index} at byte {ctx['byte_offset']}"
            )
        else:
            ctx["record_position"] = f"record #{record_index}"
    return StoreError("E_RECORD_CORRUPT", message, ctx)


def snapshot_invalid(message: str, **context: Any) -> StoreError:
    return StoreError("E_SNAPSHOT_INVALID", message, context)

