from __future__ import annotations

from typing import Any


class WorkbenchError(Exception):
    """An error with a stable machine-readable code and HTTP status."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        status_code: int = 400,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        error = {"code": self.code, "message": self.message}
        error.update(self.details)
        return {"error": error}


def head_mismatch(
    message: str,
    *,
    head_id: int | str | None = None,
    activation_head_dim: int | None = None,
    projection_shape: list[int] | tuple[int, ...] | None = None,
    shard: str | None = None,
) -> WorkbenchError:
    details: dict[str, Any] = {}
    if head_id is not None:
        details["head_id"] = head_id
    if activation_head_dim is not None:
        details["activation_head_dim"] = activation_head_dim
    if projection_shape is not None:
        details["projection_shape"] = list(projection_shape)
    if shard is not None:
        details["shard"] = shard
    return WorkbenchError("E_HEAD_MISMATCH", message, status_code=400, details=details)


def stale_version(message: str, *, cause: str, **details: Any) -> WorkbenchError:
    payload = {"cause": cause}
    payload.update(details)
    return WorkbenchError("E_STALE_VERSION", message, status_code=409, details=payload)

