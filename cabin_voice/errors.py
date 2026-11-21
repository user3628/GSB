from __future__ import annotations

from typing import Any


class CabinError(Exception):
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


def invalid_stage(message: str, **details: Any) -> CabinError:
    return CabinError("E_INVALID_STAGE", message, details=details, http_status=409)


def invalid_argument(message: str, **details: Any) -> CabinError:
    return CabinError("E_INVALID_ARGUMENT", message, details=details, http_status=422)


def invalid_interval(
    message: str,
    *,
    shard_id: str,
    zone_id: str,
    start_ms: int | None = None,
    end_ms: int | None = None,
    **details: Any,
) -> CabinError:
    payload = {
        "shard_id": shard_id,
        "zone_id": zone_id,
        "start_ms": start_ms,
        "end_ms": end_ms,
        **details,
    }
    return CabinError("E_INVALID_INTERVAL", message, details=payload, http_status=422)


def stale_version(message: str, **details: Any) -> CabinError:
    return CabinError("E_STALE_VERSION", message, details=details, http_status=409)


class PipelineStopped(Exception):
    """Raised between indivisible pipeline units after a stop request."""
