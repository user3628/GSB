from __future__ import annotations

from typing import Any


class REEError(Exception):
    """Stable business error with a machine-readable code and context."""

    def __init__(
        self,
        code: str,
        stage: str,
        message: str,
        *,
        doc_id: str | None = None,
        offset: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.stage = stage
        self.message = message
        self.doc_id = doc_id
        self.offset = offset
        self.details = details or {}

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "stage": self.stage,
            "message": self.message,
        }
        if self.doc_id is not None:
            payload["doc_id"] = self.doc_id
        if self.offset is not None:
            payload["offset"] = self.offset
        if self.details:
            payload["details"] = self.details
        return payload


def invalid_stage(stage: str) -> REEError:
    return REEError(
        "E_INVALID_STAGE",
        stage,
        f"unknown pipeline stage: {stage}",
        details={"allowed_stages": ["parse", "estimate", "extract", "validate"]},
    )


def missing_artifact(stage: str, reason: str) -> REEError:
    return REEError("E_MISSING_ARTIFACT", stage, reason)


def stale_artifact(stage: str, reason: str) -> REEError:
    return REEError("E_STALE_ARTIFACT", stage, reason)


def unsupported_version(
    stage: str,
    version: int,
    supported: int,
    *,
    doc_id: str | None = None,
) -> REEError:
    return REEError(
        "E_UNSUPPORTED_VERSION",
        stage,
        f"schema version {version} is not supported; current version is {supported}",
        doc_id=doc_id,
        details={"declared_version": version, "supported_version": supported},
    )


def align_fail(
    doc_id: str,
    stage: str,
    *,
    char_start: int,
    char_end: int,
    reason: str,
) -> REEError:
    return REEError(
        "E_ALIGN_FAIL",
        stage,
        reason,
        doc_id=doc_id,
        offset=char_start,
        details={"char_start": char_start, "char_end": char_end},
    )
