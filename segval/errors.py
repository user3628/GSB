"""Structured workbench errors shared by the CLI and HTTP API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class WorkbenchError(Exception):
    code: str
    message: str
    details: dict[str, Any] | None = None
    http_status: int = 400

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.details is not None:
            payload["details"] = self.details
        return payload


def invalid_iou_threshold(value: Any) -> WorkbenchError:
    return WorkbenchError(
        "E_INVALID_IOU_THRESHOLD",
        "IoU threshold must be a finite number in [0, 1].",
        {"value": value},
        400,
    )


def invalid_budget(message: str, details: dict[str, Any] | None = None) -> WorkbenchError:
    return WorkbenchError("E_INVALID_BUDGET", message, details, 400)


def invalid_shard(message: str, details: dict[str, Any] | None = None) -> WorkbenchError:
    return WorkbenchError("E_INVALID_SHARD", message, details, 400)


def stale_checkpoint(message: str, details: dict[str, Any] | None = None) -> WorkbenchError:
    return WorkbenchError("E_STALE_CHECKPOINT", message, details, 409)


def invalid_stage(message: str, details: dict[str, Any] | None = None) -> WorkbenchError:
    return WorkbenchError("E_INVALID_STAGE", message, details, 409)

