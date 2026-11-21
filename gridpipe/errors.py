"""Structured business errors used by every interface."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional


E_INVALID_REGION = "E_INVALID_REGION"
E_TABLE_STRUCTURE = "E_TABLE_STRUCTURE"
E_CHART_AXIS = "E_CHART_AXIS"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_WORKER = "E_WORKER"
E_INVALID_INPUT = "E_INVALID_INPUT"
E_BUDGET = "E_BUDGET"
E_IO = "E_IO"
E_BUSY = "E_BUSY"


class PipelineError(Exception):
    """A deterministic, serializable pipeline error."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        shard_id: Optional[str] = None,
        location: Optional[Iterable[Any]] = None,
        diagnostics: Optional[Dict[str, Any]] = None,
        partial: Optional[Any] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.shard_id = shard_id
        self.location = list(location) if location is not None else None
        self.diagnostics = diagnostics or {}
        self.partial = partial

    def to_dict(self) -> Dict[str, Any]:
        value: Dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.shard_id is not None:
            value["shard_id"] = self.shard_id
        if self.location is not None:
            value["location"] = self.location
        if self.diagnostics:
            value["diagnostics"] = self.diagnostics
        if self.partial is not None:
            value["partial"] = self.partial
        return value

    @classmethod
    def from_dict(cls, value: Dict[str, Any]) -> "PipelineError":
        return cls(
            str(value.get("code", "E_UNKNOWN")),
            str(value.get("message", "unknown error")),
            shard_id=value.get("shard_id"),
            location=value.get("location"),
            diagnostics=value.get("diagnostics"),
            partial=value.get("partial"),
        )


def error_payload(error: PipelineError) -> Dict[str, Any]:
    return {"ok": False, "error": error.to_dict()}


def success_payload(result: Dict[str, Any]) -> Dict[str, Any]:
    return {"ok": True, "result": result}

