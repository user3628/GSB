"""Business errors exposed by the API and reusable by the core services."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class BusinessError(Exception):
    code: str
    message: str
    http_status: int = 400
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "details": self.details,
        }


def frame_corrupt(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_FRAME_CORRUPT", message, 422, details)


def format_unsupported(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_FORMAT_UNSUPPORTED", message, 415, details)


def unit_invalid(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_UNIT_INVALID", message, 422, details)


def insufficient_samples(required: int, actual: int, **details: Any) -> BusinessError:
    return BusinessError(
        "E_INSUFFICIENT_SAMPLES",
        "有效往返时延样本不足以支撑分位判定",
        422,
        {"required_samples": required, "actual_samples": actual, **details},
    )


def conflicting_batch(batch_id: str, **details: Any) -> BusinessError:
    return BusinessError(
        "E_CONFLICTING_BATCH",
        "同一批次标识对应了不同的请求内容",
        409,
        {"batch_id": batch_id, **details},
    )


def batch_aborted(batch_id: str, **details: Any) -> BusinessError:
    return BusinessError(
        "E_BATCH_ABORTED",
        "该批次曾因进程中断而中止，请使用新的批次标识重试",
        409,
        {"batch_id": batch_id, **details},
    )


def bad_request(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_BAD_REQUEST", message, 400, details)


def not_found(message: str, **details: Any) -> BusinessError:
    return BusinessError("E_NOT_FOUND", message, 404, details)

