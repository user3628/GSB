"""Stable business errors used by every entry point."""

from __future__ import annotations

from typing import Any


class QueryError(Exception):
    """A deterministic, machine-readable query-processing failure."""

    def __init__(
        self,
        code: str,
        stage: str,
        message: str,
        *,
        query_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.stage = stage
        self.message = message
        self.query_id = query_id
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "error": {
                "code": self.code,
                "details": self.details,
                "message": self.message,
                "query_id": self.query_id,
                "stage": self.stage,
            },
        }


class RequestError(QueryError):
    def __init__(self, message: str, *, query_id: str | None = None, details: dict[str, Any] | None = None) -> None:
        super().__init__("E_BAD_REQUEST", "request", message, query_id=query_id, details=details)
