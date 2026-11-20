"""Business errors used by the quantization audit engine."""

from __future__ import annotations

from typing import Any


class AuditError(Exception):
    """A deterministic, machine-readable audit failure."""

    def __init__(self, code: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                **self.details,
            },
        }


E_QUANT_INVALID = "E_QUANT_INVALID"
E_ATTRIBUTION_GAP = "E_ATTRIBUTION_GAP"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_STALE_VERSION = "E_STALE_VERSION"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_VERSION_ROLLBACK = "E_VERSION_ROLLBACK"
E_AUDIT_TAMPERED = "E_AUDIT_TAMPERED"
E_NO_VALID_VERSION = "E_NO_VALID_VERSION"
E_INVALID_ROLLBACK = "E_INVALID_ROLLBACK"
E_INVALID_INPUT = "E_INVALID_INPUT"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_WORKSPACE_EXISTS = "E_WORKSPACE_EXISTS"
E_IO = "E_IO"

