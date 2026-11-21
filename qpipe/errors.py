from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


EXIT_CODES = {
    "E_INVALID_PARAM": 2,
    "E_NUMERIC_INPUT": 3,
    "E_TOLERANCE_EXCEEDED": 4,
    "E_STALE_CHECKPOINT": 5,
    "E_INVALID_STAGE": 6,
    "E_VERIFY_FAILED": 7,
    "E_INTERRUPTED": 75,
    "E_IO": 74,
}

HTTP_STATUS = {
    "E_INVALID_PARAM": 400,
    "E_NUMERIC_INPUT": 422,
    "E_TOLERANCE_EXCEEDED": 422,
    "E_STALE_CHECKPOINT": 409,
    "E_INVALID_STAGE": 409,
    "E_VERIFY_FAILED": 422,
    "E_INTERRUPTED": 202,
    "E_IO": 500,
}


@dataclass
class QPipeError(Exception):
    code: str
    message: str
    layer: str | None = None
    parameter: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        super().__init__(self.message)

    @property
    def exit_code(self) -> int:
        return EXIT_CODES.get(self.code, 1)

    @property
    def http_status(self) -> int:
        return HTTP_STATUS.get(self.code, 500)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.layer is not None:
            result["layer"] = self.layer
        if self.parameter is not None:
            result["parameter"] = self.parameter
        if self.details:
            result["details"] = self.details
        return result


class InvalidParam(QPipeError):
    def __init__(
        self,
        message: str,
        *,
        layer: str | None = None,
        parameter: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__("E_INVALID_PARAM", message, layer, parameter, details or {})


class NumericInput(QPipeError):
    def __init__(
        self,
        message: str,
        *,
        layer: str | None = None,
        parameter: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__("E_NUMERIC_INPUT", message, layer, parameter, details or {})


class ToleranceExceeded(QPipeError):
    def __init__(
        self,
        message: str,
        *,
        layer: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__("E_TOLERANCE_EXCEEDED", message, layer, "tolerance", details)


class StaleCheckpoint(QPipeError):
    def __init__(
        self,
        message: str,
        *,
        layer: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__("E_STALE_CHECKPOINT", message, layer, "checkpoint", details)


class InvalidStage(QPipeError):
    def __init__(
        self,
        message: str,
        *,
        layer: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__("E_INVALID_STAGE", message, layer, "stage", details)


class Interrupted(QPipeError):
    def __init__(self, message: str = "graceful stop completed", **details: Any) -> None:
        super().__init__("E_INTERRUPTED", message, None, None, details)


class VerifyFailed(QPipeError):
    def __init__(self, message: str, **details: Any) -> None:
        super().__init__("E_VERIFY_FAILED", message, None, None, details)


class QPipeIOError(QPipeError):
    def __init__(self, message: str, **details: Any) -> None:
        super().__init__("E_IO", message, None, None, details)
