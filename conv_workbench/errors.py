"""Stable business errors used by the CLI and HTTP API."""

from __future__ import annotations

from typing import Any


class WorkbenchError(Exception):
    """An error with a stable machine-readable code and structured detail."""

    http_status = 400

    def __init__(self, code: str, message: str, **detail: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.detail = detail

    def to_dict(self) -> dict[str, Any]:
        return {"ok": False, "error": {"code": self.code, "message": self.message, "detail": self.detail}}


class InvalidStageError(WorkbenchError):
    http_status = 400

    def __init__(self, message: str, **detail: Any) -> None:
        super().__init__("E_INVALID_STAGE", message, **detail)


class ShapeMismatchError(WorkbenchError):
    http_status = 422

    def __init__(self, message: str, **detail: Any) -> None:
        super().__init__("E_SHAPE_MISMATCH", message, **detail)


class StaleCheckpointError(WorkbenchError):
    http_status = 409

    def __init__(self, message: str, **detail: Any) -> None:
        super().__init__("E_STALE_CHECKPOINT", message, **detail)


class InvalidRequestError(WorkbenchError):
    http_status = 400

    def __init__(self, message: str, **detail: Any) -> None:
        super().__init__("E_INVALID_REQUEST", message, **detail)


class NotFoundError(WorkbenchError):
    http_status = 404

    def __init__(self, message: str, **detail: Any) -> None:
        super().__init__("E_NOT_FOUND", message, **detail)

