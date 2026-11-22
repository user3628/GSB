from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse


HTTP_STATUS_BY_CODE = {
    "E_BAD_REQUEST": 400,
    "E_ROUTE_NOT_FOUND": 404,
    "E_STALE_BASE": 409,
    "E_CONFLICTING_REQUEST": 409,
    "E_SNAPSHOT_INVALID": 422,
    "E_UPSTREAM_UNAVAILABLE": 502,
    "E_UPSTREAM_TIMEOUT": 504,
    "E_INTERNAL": 500,
}


@dataclass
class WorkbenchError(Exception):
    business_code: str
    message: str
    http_status: int | None = None
    details: dict[str, Any] = field(default_factory=dict)
    request_id: str | None = None

    def __post_init__(self) -> None:
        super().__init__(self.message)
        if self.http_status is None:
            self.http_status = HTTP_STATUS_BY_CODE.get(self.business_code, 500)

    def payload(self) -> dict[str, Any]:
        return {
            "ok": False,
            "business_code": self.business_code,
            "message": self.message,
            "details": self.details,
            **({"request_id": self.request_id} if self.request_id else {}),
        }


def error_response(exc: WorkbenchError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.http_status or 500,
        content=exc.payload(),
        headers={"X-Workbench-Error": exc.business_code},
    )


async def workbench_error_handler(request: Request, exc: WorkbenchError) -> JSONResponse:
    if exc.request_id is None:
        exc.request_id = request.headers.get("x-request-id") or request.headers.get(
            "idempotency-key"
        )
    return error_response(exc)
