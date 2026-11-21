"""Business error codes shared by every workbench layer."""

from __future__ import annotations

from dataclasses import dataclass


class WorkbenchError(Exception):
    def __init__(self, code: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}


@dataclass(frozen=True)
class Codes:
    CHAIN_INVALID: str = "E_CHAIN_INVALID"
    INVALID_TRANSITION: str = "E_INVALID_TRANSITION"
    BUDGET_EXCEEDED: str = "E_BUDGET_EXCEEDED"
    CONFLICTING_SESSION: str = "E_CONFLICTING_SESSION"
    CERT_EXPIRED: str = "E_CERT_EXPIRED"
    CERT_NOT_YET_VALID: str = "E_CERT_NOT_YET_VALID"
    SIGNATURE_INVALID: str = "E_SIGNATURE_INVALID"
    PATH_LOOP: str = "E_PATH_LOOP"
    ROOT_NOT_SELF_SIGNED: str = "E_ROOT_NOT_SELF_SIGNED"
    SAMPLE_NOT_FOUND: str = "E_SAMPLE_NOT_FOUND"
    RECOVERY_ABORTED: str = "E_RECOVERY_ABORTED"
    DOWNGRADE_FORBIDDEN: str = "E_DOWNGRADE_FORBIDDEN"
