"""Pydantic request/response models for the HTTP API."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field


class CreateSessionRequest(BaseModel):
    object_key: str = Field(..., min_length=1)
    min_part_size: int | None = Field(default=None, ge=1)
    max_part_size: int | None = Field(default=None, ge=1)
    expected_total_size: int | None = Field(default=None, ge=0)
    merge_strategy: Literal["fast", "full"] = "fast"
    conflict_policy: Literal["reject", "allow"] = "reject"


class SubmitPartRequest(BaseModel):
    # Base64 keeps JSON deterministic and avoids raw-binary transport issues.
    content_base64: str
    sha256: str | None = Field(default=None, min_length=64, max_length=64)


class MergeRequest(BaseModel):
    strategy: Literal["fast", "full"] | None = None
    simulate_crash_after_intent: bool = False


class ErrorEnvelope(BaseModel):
    ok: bool = False
    error: dict
