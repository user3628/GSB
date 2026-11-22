from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class FusionError(Exception):
    """Structured, machine-readable error returned by the component."""

    code: str
    message: str
    layer: str | None = None
    index: int | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
            }
        }
        if self.layer is not None:
            payload["error"]["layer"] = self.layer
        if self.index is not None:
            payload["error"]["index"] = self.index
        if self.details:
            payload["error"]["details"] = self.details
        return payload

    @classmethod
    def shape_mismatch(
        cls,
        layer: str,
        expected: Any,
        actual: Any,
        *,
        stage: str | None = None,
        message: str | None = None,
    ) -> "FusionError":
        details: dict[str, Any] = {"expected_shape": list(expected), "actual_shape": list(actual)}
        if stage:
            details["stage"] = stage
        return cls(
            "E_SHAPE_MISMATCH",
            message or f"layer {layer!r} produced shape {list(actual)}, expected {list(expected)}",
            layer=layer,
            details=details,
        )

    @classmethod
    def invalid_param(
        cls,
        layer: str,
        index: int | None,
        field_name: str,
        reason: str,
        **extra: Any,
    ) -> "FusionError":
        details = {"field": field_name, "reason": reason}
        details.update(extra)
        return cls(
            "E_INVALID_PARAM",
            f"invalid parameter {field_name} for layer {layer!r}: {reason}",
            layer=layer,
            index=index,
            details=details,
        )

    @classmethod
    def stale_checkpoint(
        cls,
        message: str,
        *,
        reason: str,
        classification: str,
        **details: Any,
    ) -> "FusionError":
        payload = {"reason": reason, "classification": classification}
        payload.update(details)
        return cls("E_STALE_CHECKPOINT", message, details=payload)

    @classmethod
    def invalid_stage(cls, message: str, *, layer: str | None = None, **details: Any) -> "FusionError":
        return cls("E_INVALID_STAGE", message, layer=layer, details=details)

    @classmethod
    def numeric(cls, layer: str, reason: str, **details: Any) -> "FusionError":
        return cls(
            "E_NUMERIC",
            f"numeric failure for layer {layer!r}: {reason}",
            layer=layer,
            details={"reason": reason, **details},
        )

    @classmethod
    def incremental_mismatch(
        cls,
        layer: str,
        *,
        index: int | None = None,
        expected: Any = None,
        actual: Any = None,
        reason: str = "artifact_diff",
        **details: Any,
    ) -> "FusionError":
        payload = {"reason": reason}
        if expected is not None:
            payload["expected"] = expected
        if actual is not None:
            payload["actual"] = actual
        payload.update(details)
        return cls(
            "E_INCREMENTAL_MISMATCH",
            f"incremental result differs from a full rebuild at layer {layer!r}",
            layer=layer,
            index=index,
            details=payload,
        )
