from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


HTTP_STATUS_BY_CODE = {
    "E_FRAME_INVALID": 400,
    "E_FLOW_CONTROL_BLOCKED": 409,
    "E_INVALID_TRANSITION": 409,
    "E_CONFLICTING_SAMPLE": 409,
    "E_SAMPLE_NOT_FOUND": 404,
    "E_CONNECTION_NOT_FOUND": 404,
    "E_STREAM_NOT_FOUND": 404,
    "E_INVALID_CONFIG": 400,
    "E_SCHEDULE_LIMIT": 409,
    "E_SCHEDULE_ABORTED": 409,
    "E_SCHEDULE_IN_PROGRESS": 409,
}


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    connection_id: str | None = None
    context: dict[str, Any] = field(default_factory=dict)
    http_status: int | None = None

    def __post_init__(self) -> None:
        super().__init__(self.message)
        if self.http_status is None:
            self.http_status = HTTP_STATUS_BY_CODE.get(self.code, 400)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "connection_id": self.connection_id,
                "context": self.context,
            }
        }
