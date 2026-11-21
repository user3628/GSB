"""Structured domain errors used by both the library and HTTP app."""

from __future__ import annotations

from typing import Any


E_MISSING_INPUT = "E_MISSING_INPUT"
E_INVALID_DATASET = "E_INVALID_DATASET"
E_PROJECTION_INVALID = "E_PROJECTION_INVALID"
E_INVALID_BITWIDTH = "E_INVALID_BITWIDTH"
E_QUANTIZATION_ZERO_SCALE = "E_QUANTIZATION_ZERO_SCALE"
E_NUMERIC = "E_NUMERIC"
E_INVALID_QUERY = "E_INVALID_QUERY"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_CORRUPT_STATE = "E_CORRUPT_STATE"
E_STOPPED = "E_STOPPED"


def _json_safe(value: Any) -> Any:
    """Keep diagnostic context while making NaN/Inf serializable as evidence."""
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value == float("inf"):
            return "Infinity"
        if value == float("-inf"):
            return "-Infinity"
        return value
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


class CMIRError(Exception):
    """A domain error with a stable code and machine-readable details."""

    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def to_dict(self, *, include_traceback: bool = False) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": _json_safe(self.details),
            },
        }
        if include_traceback:
            import traceback

            payload["error"]["traceback"] = traceback.format_exc()
        return payload


def missing_input(item_id: str, field: str, message: str, **details: Any) -> CMIRError:
    return CMIRError(
        E_MISSING_INPUT,
        message,
        item_id=item_id,
        field=field,
        **details,
    )


def numeric_error(item_id: str, stage: str, message: str, **details: Any) -> CMIRError:
    return CMIRError(
        E_NUMERIC,
        message,
        item_id=item_id,
        stage=stage,
        **details,
    )


def invalid_query(message: str, **details: Any) -> CMIRError:
    return CMIRError(E_INVALID_QUERY, message, stage="query", **details)
