from __future__ import annotations

from typing import Any


class WorkbenchError(Exception):
    """A stable, machine-readable application error."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        fragment_id: str | None = None,
        pixel: tuple[int, int] | list[int] | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.fragment_id = fragment_id
        self.pixel = list(pixel) if pixel is not None else None
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "fragment_id": self.fragment_id,
            "pixel": self.pixel,
            "details": self.details,
        }


E_FORMAT_LOSS = "E_FORMAT_LOSS"
E_NUMERIC = "E_NUMERIC"
E_STALE_SNAPSHOT = "E_STALE_SNAPSHOT"
E_SNAPSHOT_DIGEST = "E_SNAPSHOT_DIGEST"
E_VALIDATION = "E_VALIDATION"
E_NOT_FOUND = "E_NOT_FOUND"
E_LEGACY = "E_LEGACY"
E_BUSY = "E_BUSY"

