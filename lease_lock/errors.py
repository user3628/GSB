from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


ERROR_STATUS: dict[str, int] = {
    "E_LOCK_HELD": 409,
    "E_STALE_LEASE": 409,
    "E_STALE_BASE": 409,
    "E_LEASE_NOT_FOUND": 404,
    "E_DEPENDENCY_EXPIRED": 409,
    "E_DEPENDENCY_CYCLE": 409,
    "E_SNAPSHOT_NOT_FOUND": 404,
    "E_INVALID_REQUEST": 400,
    "E_INTERNAL": 500,
}


@dataclass
class ApiError(Exception):
    code: str
    message: str
    snapshot_version: int | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self, current_snapshot_version: int | None = None) -> dict[str, Any]:
        version = self.snapshot_version
        if version is None:
            version = current_snapshot_version
        body: dict[str, Any] = {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "snapshot_version": version,
        }
        if self.details:
            body["details"] = self.details
        return body

    @property
    def http_status(self) -> int:
        return ERROR_STATUS.get(self.code, 400)

