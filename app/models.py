from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class MetaRecord(Base):
    __tablename__ = "meta"

    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    value: Mapped[str] = mapped_column(Text, nullable=False)


class BatchRecord(Base):
    __tablename__ = "batches"

    batch_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(24), nullable=False, index=True)
    response_json: Mapped[str | None] = mapped_column(Text)
    reason: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class BatchEventRecord(Base):
    __tablename__ = "batch_events"

    batch_id: Mapped[str] = mapped_column(
        String(160), ForeignKey("batches.batch_id", ondelete="CASCADE"), primary_key=True
    )
    event_id: Mapped[str] = mapped_column(String(160), primary_key=True)
    sequence_no: Mapped[int] = mapped_column(Integer, nullable=False)
    timestamp_ms: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    group_key: Mapped[str] = mapped_column(String(160), nullable=False, index=True)
    measure_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    measure_scale: Mapped[int] = mapped_column(Integer, nullable=False)
    measure_scaled: Mapped[str | None] = mapped_column(Text)
    has_value: Mapped[bool] = mapped_column(Boolean, nullable=False)
    labels_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class WindowRecord(Base):
    __tablename__ = "windows"

    window_id: Mapped[str] = mapped_column(String(80), primary_key=True)
    spec_hash: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    window_kind: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    boundary: Mapped[str] = mapped_column(String(40), nullable=False)
    spec_json: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    start_ms: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    end_ms: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    group_key: Mapped[str] = mapped_column(String(160), nullable=False, index=True)
    state: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    aggregate_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_batch: Mapped[str] = mapped_column(String(160), nullable=False)
    updated_batch: Mapped[str | None] = mapped_column(String(160))
    last_event_ts: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False
    )

    __table_args__ = (
        Index("ix_windows_range", "start_ms", "end_ms", "group_key"),
        Index("ix_windows_scope", "window_kind", "group_key", "state"),
    )


class GroupTypeRecord(Base):
    __tablename__ = "group_types"

    group_key: Mapped[str] = mapped_column(String(160), primary_key=True)
    current_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    current_scale: Mapped[int] = mapped_column(Integer, nullable=False)
    last_batch_id: Mapped[str] = mapped_column(String(160), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class TypeEvolutionRecord(Base):
    __tablename__ = "type_evolution"

    group_key: Mapped[str] = mapped_column(String(160), primary_key=True)
    seq: Mapped[int] = mapped_column(Integer, primary_key=True)
    from_kind: Mapped[str | None] = mapped_column(String(16))
    from_scale: Mapped[int | None] = mapped_column(Integer)
    to_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    to_scale: Mapped[int] = mapped_column(Integer, nullable=False)
    batch_id: Mapped[str] = mapped_column(String(160), nullable=False)
    reason: Mapped[str] = mapped_column(String(80), nullable=False)
    rounding: Mapped[str] = mapped_column(String(32), nullable=False)
    basis: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (Index("ix_type_evolution_group_seq", "group_key", "seq"),)


class LateEventRecord(Base):
    __tablename__ = "late_events"

    late_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    batch_id: Mapped[str] = mapped_column(String(160), nullable=False, index=True)
    event_id: Mapped[str] = mapped_column(String(160), nullable=False)
    timestamp_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    group_key: Mapped[str] = mapped_column(String(160), nullable=False, index=True)
    measure_kind: Mapped[str] = mapped_column(String(16), nullable=False)
    measure_scale: Mapped[int] = mapped_column(Integer, nullable=False)
    measure_scaled: Mapped[str | None] = mapped_column(Text)
    labels_json: Mapped[str] = mapped_column(Text, nullable=False)
    target_window_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    target_state: Mapped[str] = mapped_column(String(16), nullable=False)
    policy: Mapped[str] = mapped_column(String(16), nullable=False)
    disposition: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)

    __table_args__ = (UniqueConstraint("batch_id", "event_id", "target_window_id", name="uq_late_event_target"),)


class SessionStateRecord(Base):
    __tablename__ = "session_state"

    spec_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    group_key: Mapped[str] = mapped_column(String(160), primary_key=True)
    window_id: Mapped[str] = mapped_column(String(80), nullable=False)
    start_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    end_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    last_event_ts: Mapped[int] = mapped_column(BigInteger, nullable=False)
    batch_id: Mapped[str] = mapped_column(String(160), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class WindowTransitionRecord(Base):
    __tablename__ = "window_transitions"

    transition_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    window_id: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    from_state: Mapped[str] = mapped_column(String(16), nullable=False)
    to_state: Mapped[str] = mapped_column(String(16), nullable=False)
    reference_id: Mapped[str] = mapped_column(String(160), nullable=False)
    reason: Mapped[str] = mapped_column(String(160), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class SnapshotRecord(Base):
    __tablename__ = "snapshots"

    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    checksum: Mapped[str] = mapped_column(String(80), nullable=False)
    snapshot_json: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="local")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class IntentRecord(Base):
    __tablename__ = "intents"

    intent_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    kind: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    ref_id: Mapped[str] = mapped_column(String(160), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    payload_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_json: Mapped[str | None] = mapped_column(Text)


class RecoveryReportRecord(Base):
    __tablename__ = "recovery_reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    report_json: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
