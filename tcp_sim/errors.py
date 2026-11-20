from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


ERROR_CATALOG: dict[str, dict[str, Any]] = {
    "E_TRACE_INVALID": {
        "http_status": 400,
        "message": "Trace record or batch order is invalid.",
    },
    "E_INVALID_TRANSITION": {
        "http_status": 409,
        "message": "The event is not legal in the connection state machine.",
    },
    "E_BATCH_NOT_FOUND": {
        "http_status": 404,
        "message": "No batch exists for the supplied batch identifier.",
    },
    "E_CHECKPOINT_CORRUPT": {
        "http_status": 500,
        "message": "Checkpoint content does not match its persisted digest.",
    },
    "E_STALE_BASE": {
        "http_status": 409,
        "message": "The batch base checkpoint is not the run's committed head.",
    },
    "E_SNAPSHOT_NOT_READY": {
        "http_status": 409,
        "message": "The requested simulation time is not yet stable and committed.",
    },
    "E_CONNECTION_NOT_FOUND": {
        "http_status": 404,
        "message": "No state exists for the requested connection.",
    },
    "E_RUN_NOT_FOUND": {
        "http_status": 404,
        "message": "No simulation run exists for the supplied identifier.",
    },
    "E_RUN_FINALIZED": {
        "http_status": 409,
        "message": "The run is finalized and cannot accept another batch.",
    },
    "E_BATCH_ABORTED": {
        "http_status": 409,
        "message": "The batch is aborted and must be resumed from a checkpoint.",
    },
    "E_IDEMPOTENCY_CONFLICT": {
        "http_status": 409,
        "message": "The batch identifier was reused with a different request.",
    },
    "E_CONFIG_MISMATCH": {
        "http_status": 409,
        "message": "The run already has a different simulator configuration.",
    },
    "E_CONFIG_INVALID": {
        "http_status": 400,
        "message": "Simulator configuration is invalid.",
    },
    "E_REQUEST_INVALID": {
        "http_status": 400,
        "message": "The HTTP request envelope is invalid.",
    },
    "E_INTERNAL": {
        "http_status": 500,
        "message": "The operation failed without committing simulation state.",
    },
}


@dataclass
class BusinessError(Exception):
    code: str
    message: str | None = None
    context: dict[str, Any] = field(default_factory=dict)
    http_status: int | None = None

    def __post_init__(self) -> None:
        catalog = ERROR_CATALOG.get(self.code, {})
        if self.message is None:
            self.message = str(catalog.get("message", "Business rule violation."))
        if self.http_status is None:
            self.http_status = int(catalog.get("http_status", 400))
        super().__init__(self.message)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
                "context": self.context,
            },
        }

    @classmethod
    def from_stored(cls, value: dict[str, Any]) -> "BusinessError":
        error = value.get("error", value)
        return cls(
            code=str(error.get("code", "E_INTERNAL")),
            message=error.get("message"),
            context=dict(error.get("context", {})),
        )
