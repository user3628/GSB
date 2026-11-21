from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AppError(Exception):
    """A business error that is always serialized with a stable error code."""

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    http_status: int = 400

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def as_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


def spec_invalid(message: str, **details: Any) -> AppError:
    return AppError("E_SPEC_INVALID", message, details, 400)


def observation_invalid(message: str, **details: Any) -> AppError:
    return AppError("E_OBSERVATION_INVALID", message, details, 400)


def stale_snapshot(message: str, **details: Any) -> AppError:
    return AppError("E_STALE_SNAPSHOT", message, details, 409)


def invalid_transition(message: str, **details: Any) -> AppError:
    return AppError("E_INVALID_TRANSITION", message, details, 409)


def scale_queue_full(message: str, **details: Any) -> AppError:
    return AppError("E_SCALE_QUEUE_FULL", message, details, 429)


def insufficient_capacity(message: str, **details: Any) -> AppError:
    return AppError("E_INSUFFICIENT_CAPACITY", message, details, 409)


def round_not_found(round_id: str) -> AppError:
    return AppError(
        "E_ROUND_NOT_FOUND",
        f"round {round_id!r} does not exist",
        {"round_id": round_id},
        404,
    )


def conflicting_round(round_id: str, **details: Any) -> AppError:
    return AppError(
        "E_CONFLICTING_ROUND",
        f"round {round_id!r} already exists with a different request",
        {"round_id": round_id, **details},
        409,
    )
