from __future__ import annotations

from typing import Any, Mapping


E_GRAPH_INVALID = "E_GRAPH_INVALID"
E_STALE_BASE = "E_STALE_BASE"
E_SNAPSHOT_STALE = "E_SNAPSHOT_STALE"
E_VOCAB_MISSING = "E_VOCAB_MISSING"
E_GRAY_RATIO_INVALID = "E_GRAY_RATIO_INVALID"
E_VERSION_NOT_FOUND = "E_VERSION_NOT_FOUND"
E_MODEL_NOT_FOUND = "E_MODEL_NOT_FOUND"
E_REQUEST_BINDING_MISMATCH = "E_REQUEST_BINDING_MISMATCH"
E_REQUEST_INTERRUPTED = "E_REQUEST_INTERRUPTED"
E_STATE_EMPTY = "E_STATE_EMPTY"
E_VERSION_CONFLICT = "E_VERSION_CONFLICT"
E_BAD_REQUEST = "E_BAD_REQUEST"
E_SNAPSHOT_INVALID = "E_SNAPSHOT_INVALID"
E_PORT_UNAVAILABLE = "E_PORT_UNAVAILABLE"
E_INTERNAL = "E_INTERNAL"


class BusinessError(Exception):
    """An expected failure with a stable machine-readable code and context."""

    def __init__(
        self,
        code: str,
        message: str,
        context: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.context = dict(context or {})

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }

