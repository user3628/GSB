"""Business error codes for the simulation platform."""
from __future__ import annotations


class BizError(Exception):
    """Business error carrying a stable machine-readable code."""

    def __init__(self, code: str, message: str, details: dict | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.details = details or {}

    def to_dict(self) -> dict:
        return {"error": {"code": self.code, "message": self.message, "details": self.details}}


# --- stable business codes -------------------------------------------------
E_TOPOLOGY_INVALID = "E_TOPOLOGY_INVALID"
E_PLAN_INVALID = "E_PLAN_INVALID"
E_FAULT_DOMAIN_FULL = "E_FAULT_DOMAIN_FULL"
E_INJECTION_QUEUE_FULL = "E_INJECTION_QUEUE_FULL"
E_INJECTION_TIMEOUT = "E_INJECTION_TIMEOUT"
E_FAILOVER_DENIED = "E_FAILOVER_DENIED"
E_INVALID_TRANSITION = "E_INVALID_TRANSITION"
E_NOT_FOUND = "E_NOT_FOUND"
E_ROUND_ABORTED = "E_ROUND_ABORTED"
E_IDEMPOTENCY_CONFLICT = "E_IDEMPOTENCY_CONFLICT"

# skip reasons recorded on skipped / deferred injection items
SKIP_QUOTA = "FAULT_DOMAIN_QUOTA_EXHAUSTED"
SKIP_CONCURRENCY = "INJECTION_CONCURRENCY_EXHAUSTED"
SKIP_QUEUE_FULL = "INJECTION_QUEUE_FULL"
SKIP_TIME_BUDGET = "ROUND_TIME_BUDGET_EXHAUSTED"
SKIP_DUPLICATE_SERVICE = "DUPLICATE_SERVICE_IN_ROUND"
SKIP_ROUND_ABORTED = "ROUND_ABORTED"
