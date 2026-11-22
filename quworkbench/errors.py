"""Stable business error codes for the query understanding workbench."""

E_EMPTY_QUERY = "E_EMPTY_QUERY"
E_INVALID_CONFIG = "E_INVALID_CONFIG"
E_SLOT_INVALID = "E_SLOT_INVALID"
E_INCREMENTAL_MISMATCH = "E_INCREMENTAL_MISMATCH"
E_STALE_CHECKPOINT = "E_STALE_CHECKPOINT"
E_INVALID_STAGE = "E_INVALID_STAGE"


class WorkbenchError(Exception):
    """Business error carrying a stable code, optional stage and reason."""

    def __init__(self, code, reason, stage=None, details=None):
        self.code = code
        self.stage = stage
        self.reason = reason
        self.details = dict(details or {})
        super().__init__(f"{code}({stage}): {reason} {self.details}")

    def to_dict(self):
        out = {"code": self.code, "reason": self.reason}
        if self.stage is not None:
            out["stage"] = self.stage
        out.update(self.details)
        return out
