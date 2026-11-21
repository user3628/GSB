from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


ERROR_HTTP_STATUS: dict[str, int] = {
    "E_RECORD_INVALID": 422,
    "E_LABEL_INVALID": 422,
    "E_SHARD_KEY_MISSING": 422,
    "E_SAMPLE_RATIO_INVALID": 422,
    "E_CONFLICTING_RECORD": 409,
    "E_STALE_SNAPSHOT": 409,
    "E_STALE_BASE": 409,
    "E_SNAPSHOT_INVALID": 422,
    "E_POLICY_NOT_FOUND": 404,
    "E_BATCH_NOT_FOUND": 404,
    "E_BATCH_NOT_COMMITTED": 409,
    "E_BATCH_ID_CONFLICT": 409,
    "E_INPUT_NOT_FOUND": 404,
    "E_INPUT_INVALID": 422,
    "E_INJECTION_INVALID": 422,
    "E_INJECTED_INTERRUPT": 409,
    "E_REPRO_MISMATCH": 409,
    "E_TENSOR_READBACK_MISMATCH": 500,
    "E_POLICY_BUSY": 409,
    "E_BATCH_WAIT_TIMEOUT": 409,
    "E_RECOVERY_ABORTED": 409,
    "E_REQUEST_INVALID": 422,
    "E_ROUTE_NOT_FOUND": 404,
    "E_METHOD_NOT_ALLOWED": 405,
    "E_INTERNAL": 500,
}


@dataclass
class PipelineError(Exception):
    code: str
    message: str
    context: dict[str, Any] = field(default_factory=dict)
    http_status: int | None = None

    def __post_init__(self) -> None:
        if self.http_status is None:
            self.http_status = ERROR_HTTP_STATUS.get(self.code, 400)
        Exception.__init__(self, self.message)

    def as_dict(self) -> dict[str, Any]:
        return {
            "error": {
                "code": self.code,
                "message": self.message,
                "context": self.context,
            }
        }


def error_from_stored(code: str, error_json: dict[str, Any] | None) -> PipelineError:
    payload = (error_json or {}).get("error", error_json or {})
    return PipelineError(
        code=code,
        message=str(payload.get("message", "batch failed")),
        context=dict(payload.get("context", {})),
    )
