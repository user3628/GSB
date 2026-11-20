"""Stable business error codes for the workbench."""


class WorkbenchError(Exception):
    def __init__(self, code: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict:
        return {"error_code": self.code, "message": self.message, "details": self.details}


def raise_error(code: str, message: str, details: dict | None = None):
    raise WorkbenchError(code, message, details)


E_EMPTY_TEXT = "E_EMPTY_TEXT"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"
E_STALE_SNAPSHOT = "E_STALE_SNAPSHOT"
E_BAD_INPUT = "E_BAD_INPUT"
E_CONFIG = "E_CONFIG"
E_BUDGET_EXCEEDED = "E_BUDGET_EXCEEDED"
