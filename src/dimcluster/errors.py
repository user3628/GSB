from __future__ import annotations

from typing import Any


HTTP_STATUS = {
    "E_INVALID_CONFIG": 400,
    "E_INVALID_STAGE": 409,
    "E_STALE_CHECKPOINT": 409,
    "E_VERSION_REPLACED": 409,
    "E_SEQUENCE_REGRESSION": 409,
    "E_DIGEST_MISMATCH": 409,
    "E_INCREMENTAL_MISMATCH": 409,
    "E_STOPPED": 409,
    "E_INTERNAL": 500,
}


class WorkbenchError(Exception):
    """An error with a stable API error code and structured details."""

    def __init__(self, code: str, message: str, details: Any | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details if details is not None else {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "details": self.details,
            },
        }

    @property
    def http_status(self) -> int:
        return HTTP_STATUS.get(self.code, 500)


def invalid_config(message: str, details: Any | None = None) -> WorkbenchError:
    return WorkbenchError("E_INVALID_CONFIG", message, details)


def invalid_stage(message: str, details: Any | None = None) -> WorkbenchError:
    return WorkbenchError("E_INVALID_STAGE", message, details)


def stale_checkpoint(message: str, details: Any | None = None) -> WorkbenchError:
    return WorkbenchError("E_STALE_CHECKPOINT", message, details)


def version_replaced(message: str, details: Any | None = None) -> WorkbenchError:
    return WorkbenchError("E_VERSION_REPLACED", message, details)


def sequence_regression(message: str, details: Any | None = None) -> WorkbenchError:
    return WorkbenchError("E_SEQUENCE_REGRESSION", message, details)


def digest_mismatch(message: str, details: Any | None = None) -> WorkbenchError:
    return WorkbenchError("E_DIGEST_MISMATCH", message, details)


def incremental_mismatch(message: str, details: Any | None = None) -> WorkbenchError:
    return WorkbenchError("E_INCREMENTAL_MISMATCH", message, details)
