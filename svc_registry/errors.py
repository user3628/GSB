from __future__ import annotations

from typing import Any


E_FORMAT_CORRUPT = "E_FORMAT_CORRUPT"
E_FORMAT_UNSUPPORTED = "E_FORMAT_UNSUPPORTED"
E_FORMAT_AMBIGUOUS = "E_FORMAT_AMBIGUOUS"
E_WEIGHT_INVALID = "E_WEIGHT_INVALID"
E_WEIGHT_POLICY_REQUIRED = "E_WEIGHT_POLICY_REQUIRED"
E_TIME_POLICY_REQUIRED = "E_TIME_POLICY_REQUIRED"
E_TIME_INVALID = "E_TIME_INVALID"
E_ENDPOINT_INVALID = "E_ENDPOINT_INVALID"
E_PROBE_METHOD_INVALID = "E_PROBE_METHOD_INVALID"
E_PROBE_PARAMS_INVALID = "E_PROBE_PARAMS_INVALID"
E_PROBE_TIMEOUT = "E_PROBE_TIMEOUT"
E_PROBE_QUEUE_FULL = "E_PROBE_QUEUE_FULL"
E_PROBE_FAILED = "E_PROBE_FAILED"
E_ROUND_BUDGET_EXCEEDED = "E_ROUND_BUDGET_EXCEEDED"
E_NOT_FOUND = "E_NOT_FOUND"
E_STALE_LEASE = "E_STALE_LEASE"
E_CONFLICTING_BATCH = "E_CONFLICTING_BATCH"
E_IMPORT_REJECTED = "E_IMPORT_REJECTED"
E_DUPLICATE = "E_DUPLICATE"
E_DB_ERROR = "E_DB_ERROR"
E_VALIDATION = "E_VALIDATION"


class BusinessError(Exception):
    """An error that is safe to expose as a business response."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        details: dict[str, Any] | None = None,
        http_status: int = 400,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}
        self.http_status = http_status

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


def corrupt(record_index: int, message: str, **details: Any) -> BusinessError:
    merged = {
        "record_index": record_index,
        "record_number": record_index + 1,
        **details,
    }
    return BusinessError(
        E_FORMAT_CORRUPT,
        f"record {record_index + 1}: {message}",
        details=merged,
        http_status=400,
    )
