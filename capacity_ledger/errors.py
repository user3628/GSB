"""Business errors and the single JSON error envelope used by the API."""

from __future__ import annotations

from typing import Any


class CapacityError(Exception):
    """A deterministic, machine-readable capacity-ledger failure."""

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

    def to_payload(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }


def snapshot_invalid(path: str, reason: str, **extra: Any) -> CapacityError:
    details: dict[str, Any] = {"path": path, "reason": reason}
    details.update(extra)
    return CapacityError(
        "E_SNAPSHOT_INVALID",
        "The snapshot or increment failed validation or its checksum does not match.",
        details=details,
        http_status=422,
    )


def stale_base(current_version: int, supplied_base_version: int) -> CapacityError:
    return CapacityError(
        "E_STALE_BASE",
        "The increment base version is not the current committed version.",
        details={
            "current_version": current_version,
            "supplied_base_version": supplied_base_version,
            "persisted_state_changed": False,
        },
        http_status=409,
    )


def conflicting_commit(commit_id: str) -> CapacityError:
    return CapacityError(
        "E_CONFLICTING_COMMIT",
        "The commit id was already used with different content.",
        details={"commit_id": commit_id},
        http_status=409,
    )


def ledger_corrupt(path: str, reason: str, **extra: Any) -> CapacityError:
    details: dict[str, Any] = {"path": path, "reason": reason}
    details.update(extra)
    return CapacityError(
        "E_LEDGER_CORRUPT",
        "Persisted ledger facts failed integrity verification.",
        details=details,
        http_status=500,
    )

