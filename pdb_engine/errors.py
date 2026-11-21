from __future__ import annotations

from typing import Any


HTTP_STATUS_BY_CODE = {
    "E_SPEC_INVALID": 400,
    "E_SELECTOR_EMPTY": 422,
    "E_PDB_BLOCKED": 409,
    "E_NO_FEASIBLE_NODE": 409,
    "E_DRAIN_NOT_FOUND": 404,
    "E_POD_NOT_FOUND": 404,
    "E_DRAIN_ABORTED": 409,
    "E_INTERNAL": 500,
}


class BusinessError(Exception):
    """An expected, machine-readable engine failure."""

    def __init__(
        self,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.http_status = http_status or HTTP_STATUS_BY_CODE.get(code, 400)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


def spec_invalid(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_SPEC_INVALID", message, details)

