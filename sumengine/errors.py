"""Structured errors used by both the library and the HTTP API."""

from __future__ import annotations

from typing import Any


class EngineError(Exception):
    """An expected engine failure with a stable machine-readable code."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        doc_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.doc_id = doc_id
        self.details = details or {}

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "ok": False,
            "error": {
                "code": self.code,
                "message": self.message,
            },
        }
        if self.doc_id is not None:
            result["error"]["doc_id"] = self.doc_id
        if self.details:
            result["error"]["details"] = self.details
        return result


def input_too_short(doc_id: str, length: int, minimum: int) -> EngineError:
    return EngineError(
        "E_INPUT_TOO_SHORT",
        f"Document body is too short: {length} < {minimum} characters",
        doc_id=doc_id,
        details={"body_chars": length, "minimum_chars": minimum},
    )
