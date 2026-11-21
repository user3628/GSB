"""Business error codes surfaced by the workbench."""

E_PLAN_INVALID = "E_PLAN_INVALID"
E_FAULT_UNSUPPORTED = "E_FAULT_UNSUPPORTED"
E_INJECTION_QUEUE_FULL = "E_INJECTION_QUEUE_FULL"
E_INJECTION_TIMEOUT = "E_INJECTION_TIMEOUT"
E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
E_ROUND_INVALID = "E_ROUND_INVALID"
E_NOT_FOUND = "E_NOT_FOUND"
E_PLAN_NOT_RUNNING = "E_PLAN_NOT_RUNNING"


class BizError(Exception):
    """Error carrying a stable business code."""

    def __init__(self, code, message, details=None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self):
        return {"code": self.code, "message": self.message, "details": self.details}
