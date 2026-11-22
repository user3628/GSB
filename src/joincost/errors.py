from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


E_FORMAT_CORRUPT = "E_FORMAT_CORRUPT"
E_FORMAT_UNSUPPORTED = "E_FORMAT_UNSUPPORTED"
E_STAT_INVALID = "E_STAT_INVALID"
E_SEARCH_SPACE_TOO_LARGE = "E_SEARCH_SPACE_TOO_LARGE"
E_VERSION_RETIRED = "E_VERSION_RETIRED"
E_CONFLICTING_REQUEST = "E_CONFLICTING_REQUEST"
E_SNAPSHOT_CONFLICT = "E_SNAPSHOT_CONFLICT"
E_NO_CONNECTED_PLAN = "E_NO_CONNECTED_PLAN"
E_REQUEST_ABORTED = "E_REQUEST_ABORTED"
E_INPUT_INVALID = "E_INPUT_INVALID"
E_NOT_FOUND = "E_NOT_FOUND"
E_INTERNAL = "E_INTERNAL"


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        Exception.__init__(self, self.message)

    @property
    def http_status(self) -> int:
        if self.code in {E_INPUT_INVALID, E_STAT_INVALID}:
            return 400
        if self.code in {E_NOT_FOUND}:
            return 404
        if self.code in {E_CONFLICTING_REQUEST, E_SNAPSHOT_CONFLICT}:
            return 409
        if self.code in {E_VERSION_RETIRED, E_REQUEST_ABORTED}:
            return 410
        if self.code in {E_FORMAT_CORRUPT, E_FORMAT_UNSUPPORTED}:
            return 422
        if self.code == E_SEARCH_SPACE_TOO_LARGE:
            return 422
        if self.code == E_NO_CONNECTED_PLAN:
            return 422
        return 500

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


def error_payload(code: str, message: str, **details: Any) -> dict[str, Any]:
    return {
        "ok": False,
        "error": {"code": code, "message": message, "details": details},
    }

