from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


HTTP_STATUS_BY_CODE: dict[str, int] = {
    "E_BAD_REQUEST": 400,
    "E_SHARD_KEY_INVALID": 400,
    "E_QUERY_NOT_SUPPORTED": 400,
    "E_SNAPSHOT_INVALID": 422,
    "E_EMPTY_PHYSICAL_MAPPING": 422,
    "E_ROUTE_CYCLE": 409,
    "E_ROUTE_CONFLICT": 409,
    "E_STALE_BASE": 409,
    "E_VERSION_ROLLBACK": 409,
    "E_REQUEST_CONFLICT": 409,
    "E_VERSION_RETIRED": 410,
    "E_EXECUTION_ABORTED": 503,
    "E_EXECUTION_FAILED": 500,
    "E_NOT_FOUND": 404,
    "E_INTERNAL": 500,
}


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    http_status: int | None = None

    def __post_init__(self) -> None:
        super().__init__(self.message)
        if self.http_status is None:
            self.http_status = HTTP_STATUS_BY_CODE.get(self.code, 400)

    def to_error(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }

    def to_envelope(self) -> dict[str, Any]:
        return {"ok": False, "error": self.to_error()}


def error(code: str, message: str, **context: Any) -> BusinessError:
    return BusinessError(code=code, message=message, context=context)


INJECTION_CODES: dict[str, str] = {
    "invalid_shard_key": "E_SHARD_KEY_INVALID",
    "empty_physical_mapping": "E_EMPTY_PHYSICAL_MAPPING",
    "version_rollback": "E_VERSION_ROLLBACK",
    "range_overlap": "E_ROUTE_CONFLICT",
    "execution_interrupt": "E_EXECUTION_ABORTED",
}


INJECTION_MESSAGES: dict[str, str] = {
    "invalid_shard_key": "injected invalid shard key",
    "empty_physical_mapping": "injected empty physical table mapping",
    "version_rollback": "injected routing version rollback",
    "range_overlap": "injected overlapping shard ranges",
    "execution_interrupt": "injected interruption during cross-table execution",
}

