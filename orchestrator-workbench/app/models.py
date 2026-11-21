"""Pydantic models shared by codecs, services and the HTTP layer."""
from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class FormatName(str, Enum):
    BINARY = "binary"
    JSONL = "jsonl"
    YAML = "yaml"


class RoundingRule(str, Enum):
    HALF_UP = "half_up"
    HALF_EVEN = "half_even"
    FLOOR = "floor"
    CEIL = "ceil"
    TRUNCATE = "truncate"


class NegotiationDecision(str, Enum):
    ACCEPT = "accept"
    DEGRADED_ACCEPT = "degraded_accept"
    REJECT = "reject"


class RawRecord(BaseModel):
    """One record as decoded from a persistence format (before normalization)."""

    resource_id: str
    parents: list[str] = Field(default_factory=list)
    budget: Optional[int] = None
    weight_permille: Optional[int] = None
    weight_float: Optional[float] = None
    expected_version: Optional[int] = None
    present_fields: list[str] = Field(default_factory=list)


class SnapshotHeader(BaseModel):
    """Metadata carried by every snapshot payload."""

    format: FormatName
    format_version: int
    snapshot_seq: int
    generated_at: str
    unknown_required_fields: list[str] = Field(default_factory=list)


class DecodedPayload(BaseModel):
    header: SnapshotHeader
    records: list[RawRecord]


class RoundingInfo(BaseModel):
    resource_id: str
    input_weight: float
    rounding_rule: RoundingRule
    discarded_fraction: float
    rounded_permille: int
    cumulative_error: float


class NormalizedResource(BaseModel):
    resource_id: str
    budget: int
    weight_permille: int
    parents: list[str]
    expected_version: int
    layer: int = -1
    batch: int = -1


class RejectedEntry(BaseModel):
    resource_id: str
    reason: str


class DegradationEntry(BaseModel):
    resource_id: str
    field: str
    reason: str
    applied_value: Optional[float] = None


class NegotiationReport(BaseModel):
    format: FormatName
    format_version: int
    decision: NegotiationDecision
    matrix_rule: str
    reasons: list[str] = Field(default_factory=list)
    degradations: list[DegradationEntry] = Field(default_factory=list)
    unknown_required_fields: list[str] = Field(default_factory=list)


class ImportReport(BaseModel):
    snapshot_seq: int
    format: FormatName
    format_version: int
    negotiation: NegotiationReport
    accepted: int
    rejected: list[RejectedEntry] = Field(default_factory=list)
    rounding: list[RoundingInfo] = Field(default_factory=list)
    cumulative_rounding_error: float
    layer_count: int
    batch_count: int


class LayerBatch(BaseModel):
    layer: int
    batch: int
    resources: list[str]


class SnapshotStats(BaseModel):
    snapshot_seq: int
    generated_at: str
    resource_count: int
    edge_count: int
    layer_count: int
    batch_count: int


class IncrementFragment(BaseModel):
    base_snapshot_seq: int
    submit_id: str
    changes: list[RawRecord]
    removed_ids: list[str] = Field(default_factory=list)
    rounding_rule: RoundingRule = RoundingRule.HALF_EVEN


class IncrementResult(BaseModel):
    submit_id: str
    base_snapshot_seq: int
    new_snapshot_seq: int
    affected_resources: list[str]
    affected_layers: list[int]
    affected_batches: list[int]
    impact_downstream: list[str]


class RecoveryIntent(BaseModel):
    intent_id: str
    kind: str
    status: str
    detail: str
    created_at: str
    finished_at: Optional[str] = None


class RecoveryReport(BaseModel):
    recovered: bool
    unfinished_intents: list[RecoveryIntent]
    aborted_intents: list[RecoveryIntent]
    active_snapshot_seq: Optional[int]
    note: str
