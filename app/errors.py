from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class BusinessError(Exception):
    code: str
    message: str
    http_status: int = 400
    details: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return f"{self.code}: {self.message}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            }
        }


def snapshot_invalid(message: str, **details: Any) -> BusinessError:
    return BusinessError(
        code="E_SNAPSHOT_INVALID",
        message=message,
        http_status=422,
        details=details,
    )


def stale_base(expected: int, received: int | None) -> BusinessError:
    return BusinessError(
        code="E_STALE_BASE",
        message="base_version is not the current committed version",
        http_status=409,
        details={"current_version": expected, "base_version": received},
    )


def duplicate_version(version: int, reason: str) -> BusinessError:
    return BusinessError(
        code="E_DUPLICATE_VERSION",
        message="snapshot version already exists with different content",
        http_status=409,
        details={"snapshot_version": version, "reason": reason},
    )


def version_not_found(version: int) -> BusinessError:
    return BusinessError(
        code="E_VERSION_NOT_FOUND",
        message="no complete committed snapshot exists for this version",
        http_status=404,
        details={"snapshot_version": version},
    )
