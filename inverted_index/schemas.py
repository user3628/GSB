from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class BatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    batch_id: str | None = None
    inserts: list[dict[str, Any]] = Field(default_factory=list)
    updates: list[dict[str, Any]] = Field(default_factory=list)
    deletes: list[str] = Field(default_factory=list)


class TermClause(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["term"]
    field: str
    term: str


class PhraseClause(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["phrase"]
    field: str
    terms: list[str] = Field(min_length=1)


SearchClauseModel = Annotated[TermClause | PhraseClause, Field(discriminator="kind")]


class SearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int | None = Field(default=None, ge=0)
    operator: Literal["AND", "OR"] = "AND"
    clauses: list[SearchClauseModel] = Field(min_length=1)
    limit: int = Field(default=20, ge=1, le=10_000)
    offset: int = Field(default=0, ge=0)


class MergeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_segment_ids: list[str] | None = None


class RetireRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    retain_versions: int = Field(default=3, ge=1)
