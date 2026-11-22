from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    http_status: int = 400
    dns_rcode: int = 2

    def __post_init__(self) -> None:
        super().__init__(f"{self.code}: {self.message}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "context": self.context,
            }
        }


def api_error(
    code: str,
    message: str,
    *,
    http_status: int = 400,
    dns_rcode: int = 2,
    **context: Any,
) -> BusinessError:
    return BusinessError(
        code=code,
        message=message,
        context=context,
        http_status=http_status,
        dns_rcode=dns_rcode,
    )


HTTP_STATUS_BY_CODE = {
    "E_MESSAGE_INVALID": 400,
    "E_REQUEST_INVALID": 422,
    "E_CONFLICTING_QUERY": 409,
    "E_QUEUE_FULL": 429,
    "E_BUDGET_EXHAUSTED": 503,
    "E_UPSTREAM_TIMEOUT": 504,
    "E_UPSTREAM_INVALID": 502,
    "E_CNAME_LOOP": 502,
    "E_CHAIN_TOO_LONG": 502,
    "E_NO_AUTHORITY": 502,
    "E_CACHE_MISS": 404,
    "E_SESSION_NOT_FOUND": 404,
    "E_ROUND_NOT_FOUND": 404,
    "E_RECOVERY_NOT_FOUND": 404,
    "E_STORAGE": 500,
    "E_INTERNAL": 500,
}

