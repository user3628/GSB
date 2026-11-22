from __future__ import annotations

import hashlib
import json
import os
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session

from .aggregate import merge_value, new_aggregate, output_aggregate
from .db import Database
from .errors import EngineError
from .models import (
    BatchEventRecord,
    BatchRecord,
    GroupTypeRecord,
    IntentRecord,
    LateEventRecord,
    MetaRecord,
    RecoveryReportRecord,
    SessionStateRecord,
    SnapshotRecord,
    TypeEvolutionRecord,
    WindowRecord,
    WindowTransitionRecord,
    utcnow,
)
from .numeric import ScaledValue, format_measure, parse_event_measure
from .schemas import (
    BatchRequest,
    CloseWindowsRequest,
    EmitWindowsRequest,
    EventInput,
    MeasureType,
    TransitionWindowRequest,
    WindowSpec,
    canonical_json,
    spec_from_dict,
)
from .windows import (
    WindowBounds,
    candidate_windows,
    contains,
    make_window_id,
    overlaps,
    unique_windows,
)

SNAPSHOT_FORMAT = "sqlite-stream-window-aggregator/v1"
STATE_MACHINE = {
    "OPEN": {"CLOSED"},
    "CLOSED": {"EMITTED"},
    "EMITTED": set(),
}
ALL_STATES = {"OPEN", "CLOSED", "EMITTED"}


@dataclass(frozen=True)
class ParsedEvent:
    event: EventInput
    value: ScaledValue | None
    original_index: int


def _json(value: Any) -> str:
    return canonical_json(value)


def _parse_json(value: str | None, default: Any = None) -> Any:
    if value is None:
        return default
    return json.loads(value)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _sha256(value: Any) -> str:
    return "sha256:" + hashlib.sha256(_json(value).encode()).hexdigest()


class Engine:
    """Single-writer SQLite execution engine with exact fixed-point aggregation."""

    def __init__(
        self,
        db_url: str,
        *,
        recover_on_startup: bool = True,
        crash_point: str | None = None,
    ) -> None:
        self.db = Database(db_url)
        self.db.create_schema()
        self._writer_lock = threading.RLock()
        self.crash_point = crash_point
        self.last_recovery_report: dict[str, Any] | None = None
        self._initialize_meta()
        if recover_on_startup:
            self.last_recovery_report = self.recover_pending_intents(reason="startup")

    def close(self) -> None:
        self.db.dispose()

    # ------------------------------------------------------------------ meta
    def _initialize_meta(self) -> None:
        defaults = {
            "state_version": "0",
            "snapshot_sequence": "0",
            "loaded_snapshot_version": "0",
        }
        with self.db.write_session() as session:
            for key, value in defaults.items():
                if session.get(MetaRecord, key) is None:
                    session.add(MetaRecord(key=key, value=value))

    @staticmethod
    def _meta(session: Session, key: str, default: str = "0") -> str:
        record = session.get(MetaRecord, key)
        return record.value if record else default

    @staticmethod
    def _set_meta(session: Session, key: str, value: str) -> None:
        record = session.get(MetaRecord, key)
        if record is None:
            session.add(MetaRecord(key=key, value=value))
        else:
            record.value = value

    def _state_version(self, session: Session) -> int:
        return int(self._meta(session, "state_version", "0"))

    def _next_state_version(self, session: Session) -> int:
        return self._state_version(session) + 1

    def _set_state_version(self, session: Session, value: int) -> None:
        self._set_meta(session, "state_version", str(value))

    def _maybe_crash(self, point: str, override: str | None = None) -> None:
        selected = override or self.crash_point
        if selected == point:
            os._exit(86)

    # --------------------------------------------------------------- intents
    def _write_intent(self, kind: str, ref_id: str, payload: dict[str, Any]) -> str:
        intent_id = "int_" + uuid.uuid4().hex
        with self.db.write_session() as session:
            session.add(
                IntentRecord(
                    intent_id=intent_id,
                    kind=kind,
                    ref_id=ref_id,
                    status="PENDING",
                    payload_json=_json(payload),
                )
            )
        return intent_id

    def _finish_intent(self, session: Session, intent_id: str, status: str, error: dict[str, Any] | None = None) -> None:
        record = session.get(IntentRecord, intent_id)
        if record is None:
            return
        record.status = status
        record.finished_at = utcnow()
        if error is not None:
            record.error_json = _json(error)

    def _abort_intent(self, intent_id: str, error: EngineError | None = None) -> None:
        try:
            with self.db.write_session() as session:
                self._finish_intent(
                    session,
                    intent_id,
                    "ABORTED",
                    error.to_dict() if error else {"reason": "transaction_rolled_back"},
                )
        except Exception:
            # A crash-recovery scan can still mark a pending intent later.
            pass

    def _pending_intent_for(self, session: Session, kind: str, ref_id: str) -> IntentRecord | None:
        return session.scalar(
            select(IntentRecord).where(
                IntentRecord.kind == kind,
                IntentRecord.ref_id == ref_id,
                IntentRecord.status == "PENDING",
            )
        )

    # ---------------------------------------------------------- batch ingest
    def _prepare_batch(self, request: BatchRequest) -> list[ParsedEvent]:
        ids = [event.event_id for event in request.events]
        if len(ids) != len(set(ids)):
            raise EngineError(
                "E_DUPLICATE_EVENT",
                "event_id must be unique within a batch",
                {"batch_id": request.batch_id},
            )
        stamps = [event.timestamp_ms for event in request.events]
        if request.timestamp_order == "reject" and stamps != sorted(stamps):
            raise EngineError(
                "E_OUT_OF_ORDER",
                "batch timestamps are not monotonic and timestamp_order=reject",
                {"batch_id": request.batch_id, "timestamps": stamps},
            )

        prepared = [
            ParsedEvent(
                event=event,
                value=parse_event_measure(
                    event.measure,
                    request.measure_type.kind,
                    request.measure_type.scale or 0,
                    batch_id=request.batch_id,
                    event_id=event.event_id,
                    event_index=index,
                ),
                original_index=index,
            )
            for index, event in enumerate(request.events)
        ]
        if request.timestamp_order == "sort":
            prepared.sort(key=lambda item: (item.event.timestamp_ms, item.event.event_id, item.original_index))
        return prepared

    def ingest_batch(self, request: BatchRequest, *, crash_point: str | None = None) -> dict[str, Any]:
        prepared = self._prepare_batch(request)
        request_hash = request.request_hash()

        with self._writer_lock:
            with self.db.read_session() as session:
                existing = session.get(BatchRecord, request.batch_id)
                if existing is not None:
                    return self._existing_batch_result(existing, request_hash)
                if self._pending_intent_for(session, "batch", request.batch_id) is not None:
                    raise EngineError(
                        "E_BATCH_IN_PROGRESS",
                        "another writer is processing this batch",
                        {"batch_id": request.batch_id},
                    )

            payload = {
                "batch_id": request.batch_id,
                "request_hash": request_hash,
                "event_count": len(request.events),
            }
            intent_id = self._write_intent("batch", request.batch_id, payload)
            self._maybe_crash("after_intent", crash_point)

            try:
                with self.db.write_session() as session:
                    response = self._apply_batch(session, request, prepared, request_hash, intent_id, crash_point)
                return response
            except EngineError as exc:
                self._abort_intent(intent_id, exc)
                raise
            except Exception as exc:
                self._abort_intent(
                    intent_id,
                    EngineError("E_INTERNAL", "batch transaction failed", {"error": str(exc)}),
                )
                raise

    @staticmethod
    def _existing_batch_result(existing: BatchRecord, request_hash: str) -> dict[str, Any]:
        if existing.request_hash != request_hash:
            raise EngineError(
                "E_CONFLICTING_BATCH",
                "batch_id was already used with different content",
                {
                    "batch_id": existing.batch_id,
                    "existing_status": existing.status,
                    "existing_request_hash": existing.request_hash,
                    "submitted_request_hash": request_hash,
                },
            )
        if existing.status == "COMMITTED" and existing.response_json:
            response = _parse_json(existing.response_json)
            response["deduplicated"] = True
            return response
        if existing.status == "ABORTED":
            raise EngineError(
                "E_BATCH_ABORTED",
                "batch was recovered as aborted and cannot produce state",
                {"batch_id": existing.batch_id, "reason": existing.reason},
            )
        raise EngineError(
            "E_BATCH_IN_PROGRESS",
            "batch is not in a reusable terminal state",
            {"batch_id": existing.batch_id, "status": existing.status},
        )

    def _apply_batch(
        self,
        session: Session,
        request: BatchRequest,
        prepared: list[ParsedEvent],
        request_hash: str,
        intent_id: str,
        crash_point: str | None,
    ) -> dict[str, Any]:
        existing = session.get(BatchRecord, request.batch_id)
        if existing is not None:
            return self._existing_batch_result(existing, request_hash)

        spec = request.window_spec
        measure_type = request.measure_type
        declared_scale = measure_type.scale or 0
        next_version = self._next_state_version(session)
        touched_windows: set[str] = set()
        merged_event_indices: set[int] = set()
        late_events: list[dict[str, Any]] = []
        auto_transitions: list[dict[str, Any]] = []

        type_transitions = self._apply_group_type_evolution(session, request, prepared, declared_scale)

        if spec.kind in {"tumbling", "sliding"}:
            for item in prepared:
                candidates = unique_windows(candidate_windows(spec, item.event.timestamp_ms))
                merged_any = False
                for bounds in candidates:
                    window_id = make_window_id(
                        spec.spec_hash,
                        spec.kind,
                        item.event.group_key,
                        bounds.start_ms,
                        bounds.end_ms,
                    )
                    record = session.get(WindowRecord, window_id)
                    if record is None:
                        aggregate = new_aggregate(measure_type)
                        merge_value(
                            aggregate,
                            item.value.scaled if item.value else None,
                            item.value.scale if item.value else declared_scale,
                            measure_type.kind,
                            declared_scale,
                        )
                        record = WindowRecord(
                            window_id=window_id,
                            spec_hash=spec.spec_hash,
                            window_kind=spec.kind,
                            boundary=spec.boundary,
                            spec_json=_json(spec.canonical()),
                            start_ms=bounds.start_ms,
                            end_ms=bounds.end_ms,
                            group_key=item.event.group_key,
                            state="OPEN",
                            version=1,
                            aggregate_json=_json(aggregate),
                            created_batch=request.batch_id,
                            updated_batch=request.batch_id,
                            last_event_ts=item.event.timestamp_ms,
                        )
                        session.add(record)
                        touched_windows.add(window_id)
                        merged_any = True
                    elif record.state == "OPEN":
                        aggregate = _parse_json(record.aggregate_json)
                        merge_value(
                            aggregate,
                            item.value.scaled if item.value else None,
                            item.value.scale if item.value else declared_scale,
                            measure_type.kind,
                            declared_scale,
                        )
                        record.aggregate_json = _json(aggregate)
                        record.version += 1
                        record.updated_batch = request.batch_id
                        if record.last_event_ts is None or item.event.timestamp_ms > record.last_event_ts:
                            record.last_event_ts = item.event.timestamp_ms
                        touched_windows.add(window_id)
                        merged_any = True
                    else:
                        self._handle_late_event(
                            session,
                            request,
                            item,
                            record,
                            declared_scale,
                            late_events,
                        )
                if merged_any:
                    merged_event_indices.add(item.original_index)
        else:
            for item in prepared:
                merged_any, closed = self._apply_session_event(
                    session,
                    request,
                    item,
                    next_version,
                    touched_windows,
                    late_events,
                )
                auto_transitions.extend(closed)
                if merged_any:
                    merged_event_indices.add(item.original_index)

        timestamp_order_applied = (
            "sorted_by_timestamp_then_event_id" if request.timestamp_order == "sort" else "validated_monotonic"
        )
        response: dict[str, Any] = {
            "batch_id": request.batch_id,
            "status": "COMMITTED",
            "committed": True,
            "deduplicated": False,
            "state_version": next_version,
            "measure_type": measure_type.model_dump(mode="json"),
            "window_spec": spec.canonical(),
            "window_boundary": spec.boundary_echo,
            "timestamp_order": request.timestamp_order,
            "timestamp_order_applied": timestamp_order_applied,
            "late_policy": request.late_policy,
            "event_count": len(prepared),
            "merged_event_count": len(merged_event_indices),
            "late_event_count": len(late_events),
            "late_events": late_events,
            "type_transitions": type_transitions,
            "auto_transitions": auto_transitions,
            "window_ids": sorted(touched_windows),
        }

        self._set_state_version(session, next_version)
        session.add(
            BatchRecord(
                batch_id=request.batch_id,
                request_hash=request_hash,
                status="COMMITTED",
                response_json=_json(response),
                reason=None,
                finished_at=utcnow(),
            )
        )
        session.flush()
        for item in prepared:
            session.add(
                BatchEventRecord(
                    batch_id=request.batch_id,
                    event_id=item.event.event_id,
                    sequence_no=item.original_index,
                    timestamp_ms=item.event.timestamp_ms,
                    group_key=item.event.group_key,
                    measure_kind=measure_type.kind,
                    measure_scale=declared_scale,
                    measure_scaled=str(item.value.scaled) if item.value else None,
                    has_value=item.value is not None,
                    labels_json=_json(item.event.labels),
                )
            )
        self._finish_intent(session, intent_id, "COMPLETED")
        session.flush()
        self._maybe_crash("mid_transaction", crash_point)
        return response

    def _apply_group_type_evolution(
        self,
        session: Session,
        request: BatchRequest,
        prepared: list[ParsedEvent],
        declared_scale: int,
    ) -> list[dict[str, Any]]:
        transitions: list[dict[str, Any]] = []
        new_type = request.measure_type
        for group_key in sorted({item.event.group_key for item in prepared}):
            current = session.get(GroupTypeRecord, group_key)
            from_kind = current.current_kind if current else None
            from_scale = current.current_scale if current else None
            same = current is not None and from_kind == new_type.kind and from_scale == declared_scale

            if not same:
                if current is None:
                    reason = "initial_declaration"
                    action = "INITIAL"
                else:
                    widened = declared_scale > (from_scale or 0)
                    if from_kind == "integer" and new_type.kind == "decimal":
                        widened = True
                    elif from_kind == "decimal" and new_type.kind == "integer":
                        widened = False
                    reason = "exact_promotion" if widened else "exact_narrowing_of_future_inputs"
                    action = "PROMOTE" if widened else "DEMOTE"

                sequence_no = int(
                    session.scalar(
                        select(func.max(TypeEvolutionRecord.seq)).where(TypeEvolutionRecord.group_key == group_key)
                    )
                    or 0
                ) + 1
                basis = (
                    "Aggregate state remains an exact fixed-point integer. "
                    "Existing values are multiplied by powers of ten when the scale widens; "
                    "narrower declarations affect future input validation only. Rounding is never applied."
                )
                session.add(
                    TypeEvolutionRecord(
                        group_key=group_key,
                        seq=sequence_no,
                        from_kind=from_kind,
                        from_scale=from_scale,
                        to_kind=new_type.kind,
                        to_scale=declared_scale,
                        batch_id=request.batch_id,
                        reason=reason,
                        rounding="none",
                        basis=basis,
                    )
                )
                transitions.append(
                    {
                        "group_key": group_key,
                        "sequence": sequence_no,
                        "action": action,
                        "from": None if from_kind is None else {"kind": from_kind, "scale": from_scale},
                        "to": {"kind": new_type.kind, "scale": declared_scale},
                        "reason": reason,
                        "rounding": "none",
                        "basis": basis,
                    }
                )

            if current is None:
                session.add(
                    GroupTypeRecord(
                        group_key=group_key,
                        current_kind=new_type.kind,
                        current_scale=declared_scale,
                        last_batch_id=request.batch_id,
                    )
                )
            else:
                current.current_kind = new_type.kind
                current.current_scale = declared_scale
                current.last_batch_id = request.batch_id
                current.updated_at = utcnow()
        return transitions

    def _handle_late_event(
        self,
        session: Session,
        request: BatchRequest,
        item: ParsedEvent,
        record: WindowRecord,
        declared_scale: int,
        late_events: list[dict[str, Any]],
    ) -> None:
        context = {
            "batch_id": request.batch_id,
            "event_id": item.event.event_id,
            "group_key": item.event.group_key,
            "timestamp_ms": item.event.timestamp_ms,
            "target_window_id": record.window_id,
            "target_state": record.state,
            "late_policy": request.late_policy,
        }
        if request.late_policy == "reject":
            raise EngineError(
                "E_LATE_EVENT",
                "event targets a non-OPEN window and late_policy=reject",
                context,
            )

        measure = item.value.scaled if item.value else None
        late = LateEventRecord(
            late_id="late_" + uuid.uuid4().hex,
            batch_id=request.batch_id,
            event_id=item.event.event_id,
            timestamp_ms=item.event.timestamp_ms,
            group_key=item.event.group_key,
            measure_kind=request.measure_type.kind,
            measure_scale=declared_scale,
            measure_scaled=str(measure) if measure is not None else None,
            labels_json=_json(item.event.labels),
            target_window_id=record.window_id,
            target_state=record.state,
            policy=request.late_policy,
            disposition="QUEUED",
            reason="window is not OPEN; aggregate left unchanged and event isolated in late queue",
        )
        session.add(late)
        late_events.append(
            {
                **context,
                "late_id": late.late_id,
                "disposition": "QUEUED",
                "measure": format_measure(measure, declared_scale) if measure is not None else None,
            }
        )

    def _apply_session_event(
        self,
        session: Session,
        request: BatchRequest,
        item: ParsedEvent,
        next_version: int,
        touched_windows: set[str],
        late_events: list[dict[str, Any]],
    ) -> tuple[bool, list[dict[str, Any]]]:
        spec = request.window_spec
        assert spec.gap_ms is not None
        group_key = item.event.group_key
        timestamp = item.event.timestamp_ms
        declared_scale = request.measure_type.scale or 0

        windows = list(
            session.scalars(
                select(WindowRecord)
                .where(
                    WindowRecord.spec_hash == spec.spec_hash,
                    WindowRecord.window_kind == "session",
                    WindowRecord.group_key == group_key,
                )
                .order_by(WindowRecord.start_ms)
            )
        )

        open_container = None
        any_container = None
        for record in windows:
            if contains(spec, record.start_ms, record.end_ms, timestamp):
                any_container = record
                if record.state == "OPEN":
                    open_container = record
                    break

        closed_transitions: list[dict[str, Any]] = []

        if open_container is not None:
            aggregate = _parse_json(open_container.aggregate_json)
            merge_value(
                aggregate,
                item.value.scaled if item.value else None,
                item.value.scale if item.value else declared_scale,
                request.measure_type.kind,
                declared_scale,
            )
            open_container.aggregate_json = _json(aggregate)
            open_container.version += 1
            open_container.updated_batch = request.batch_id
            if timestamp > open_container.last_event_ts:
                open_container.last_event_ts = timestamp
                open_container.end_ms = timestamp + spec.gap_ms
            touched_windows.add(open_container.window_id)
            self._update_session_current(session, request, open_container)
            return True, []

        if any_container is not None:
            self._handle_late_event(session, request, item, any_container, declared_scale, late_events)
            return False, []

        # Out-of-order events may still belong to an existing session even when
        # they precede its start: extending the start backwards preserves the
        # idle-gap semantics without creating overlapping session windows.
        def within_backward_gap(record: WindowRecord) -> bool:
            distance = record.start_ms - timestamp
            if distance <= 0:
                return False
            return distance <= spec.gap_ms if spec.end_inclusive else distance < spec.gap_ms

        backward_open = next(
            (record for record in windows if record.state == "OPEN" and within_backward_gap(record)),
            None,
        )
        if backward_open is not None:
            aggregate = _parse_json(backward_open.aggregate_json)
            merge_value(
                aggregate,
                item.value.scaled if item.value else None,
                item.value.scale if item.value else declared_scale,
                request.measure_type.kind,
                declared_scale,
            )
            backward_open.aggregate_json = _json(aggregate)
            backward_open.start_ms = timestamp
            backward_open.version += 1
            backward_open.updated_batch = request.batch_id
            touched_windows.add(backward_open.window_id)
            self._update_session_current(session, request, backward_open)
            return True, []

        backward_closed = next(
            (record for record in windows if record.state != "OPEN" and within_backward_gap(record)),
            None,
        )
        if backward_closed is not None:
            self._handle_late_event(session, request, item, backward_closed, declared_scale, late_events)
            return False, []

        # A monotonically later timestamp starts a new session; seal sessions that
        # the new timestamp has passed according to the declared boundary.
        for record in windows:
            if record.state != "OPEN" or record.start_ms >= timestamp:
                continue
            past_end = timestamp > record.end_ms if spec.end_inclusive else timestamp >= record.end_ms
            if past_end:
                old_state = record.state
                self._transition_window(
                    session,
                    record,
                    "CLOSED",
                    request.batch_id,
                    "session_idle_gap",
                )
                closed_transitions.append(
                    {
                        "window_id": record.window_id,
                        "from_state": old_state,
                        "to_state": "CLOSED",
                        "reason": "session_idle_gap",
                    }
                )

        window_id = make_window_id(spec.spec_hash, "session", group_key, timestamp)
        existing = session.get(WindowRecord, window_id)
        if existing is not None:
            self._handle_late_event(session, request, item, existing, declared_scale, late_events)
            return False, closed_transitions

        aggregate = new_aggregate(request.measure_type)
        merge_value(
            aggregate,
            item.value.scaled if item.value else None,
            item.value.scale if item.value else declared_scale,
            request.measure_type.kind,
            declared_scale,
        )
        record = WindowRecord(
            window_id=window_id,
            spec_hash=spec.spec_hash,
            window_kind="session",
            boundary=spec.boundary,
            spec_json=_json(spec.canonical()),
            start_ms=timestamp,
            end_ms=timestamp + spec.gap_ms,
            group_key=group_key,
            state="OPEN",
            version=1,
            aggregate_json=_json(aggregate),
            created_batch=request.batch_id,
            updated_batch=request.batch_id,
            last_event_ts=timestamp,
        )
        session.add(record)
        touched_windows.add(window_id)
        self._update_session_current(session, request, record)
        return True, closed_transitions

    def _update_session_current(self, session: Session, request: BatchRequest, record: WindowRecord) -> None:
        spec = request.window_spec
        current = session.get(SessionStateRecord, (spec.spec_hash, record.group_key))
        if current is None or current.window_id == record.window_id or record.start_ms >= current.start_ms:
            if current is None:
                current = SessionStateRecord(
                    spec_hash=spec.spec_hash,
                    group_key=record.group_key,
                    window_id=record.window_id,
                    start_ms=record.start_ms,
                    end_ms=record.end_ms,
                    last_event_ts=record.last_event_ts or record.start_ms,
                    batch_id=request.batch_id,
                )
                session.add(current)
            else:
                current.window_id = record.window_id
                current.start_ms = record.start_ms
                current.end_ms = record.end_ms
                current.last_event_ts = record.last_event_ts or record.start_ms
                current.batch_id = request.batch_id
                current.updated_at = utcnow()

    # ---------------------------------------------------------- state machine
    def _transition_window(
        self,
        session: Session,
        record: WindowRecord,
        target_state: str,
        reference_id: str,
        reason: str,
    ) -> None:
        from_state = record.state
        if target_state not in ALL_STATES:
            raise EngineError(
                "E_INVALID_TRANSITION",
                "unknown target window state",
                {"window_id": record.window_id, "target_state": target_state},
            )
        if target_state not in STATE_MACHINE.get(from_state, set()):
            raise EngineError(
                "E_INVALID_TRANSITION",
                "window state transition is not legal",
                {
                    "window_id": record.window_id,
                    "from_state": from_state,
                    "target_state": target_state,
                    "allowed_targets": sorted(STATE_MACHINE.get(from_state, set())),
                },
            )
        record.state = target_state
        record.version += 1
        record.updated_batch = reference_id
        session.add(
            WindowTransitionRecord(
                transition_id="tr_" + uuid.uuid4().hex,
                window_id=record.window_id,
                from_state=from_state,
                to_state=target_state,
                reference_id=reference_id,
                reason=reason,
            )
        )

    def transition_window(self, request: TransitionWindowRequest) -> dict[str, Any]:
        with self._writer_lock:
            intent_id = self._write_intent(
                "window_transition",
                request.window_id,
                {"window_id": request.window_id, "target_state": request.target_state},
            )
            try:
                with self.db.write_session() as session:
                    record = session.get(WindowRecord, request.window_id)
                    if record is None:
                        raise EngineError(
                            "E_NOT_FOUND",
                            "window does not exist",
                            {"window_id": request.window_id},
                        )
                    from_state = record.state
                    self._transition_window(
                        session,
                        record,
                        request.target_state,
                        "manual:" + request.window_id,
                        "manual_transition",
                    )
                    next_version = self._next_state_version(session)
                    self._set_state_version(session, next_version)
                    self._finish_intent(session, intent_id, "COMPLETED")
                    response = {
                        "window_id": record.window_id,
                        "from_state": from_state,
                        "to_state": request.target_state,
                        "state_version": next_version,
                    }
                return response
            except EngineError as exc:
                self._abort_intent(intent_id, exc)
                raise

    # ---------------------------------------------------------- close / emit
    def close_windows(self, request: CloseWindowsRequest) -> dict[str, Any]:
        return self._close_or_emit(request, emit=False)

    def emit_windows(self, request: EmitWindowsRequest) -> dict[str, Any]:
        return self._close_or_emit(request, emit=True)

    def _close_or_emit(self, request: CloseWindowsRequest | EmitWindowsRequest, emit: bool) -> dict[str, Any]:
        ref_id = "close" if not emit else "emit"
        with self._writer_lock:
            intent_id = self._write_intent(
                "window_emit" if emit else "window_close",
                f"{ref_id}:{request.through_ms}",
                request.model_dump(mode="json"),
            )
            try:
                with self.db.write_session() as session:
                    transitions: list[dict[str, Any]] = []

                    def rows_for(state: str) -> list[WindowRecord]:
                        stmt = select(WindowRecord).where(
                            WindowRecord.state == state,
                            WindowRecord.end_ms <= request.through_ms,
                        )
                        if request.window_kind is not None:
                            stmt = stmt.where(WindowRecord.window_kind == request.window_kind)
                        if request.group_key is not None:
                            stmt = stmt.where(WindowRecord.group_key == request.group_key)
                        return list(session.scalars(stmt.order_by(WindowRecord.start_ms, WindowRecord.window_id)))

                    if emit:
                        for record in rows_for("OPEN"):
                            old_state = record.state
                            self._transition_window(session, record, "CLOSED", ref_id, "emit_precondition_close")
                            transitions.append(
                                {
                                    "window_id": record.window_id,
                                    "from_state": old_state,
                                    "to_state": "CLOSED",
                                    "reason": "emit_precondition_close",
                                }
                            )
                        for record in rows_for("CLOSED"):
                            old_state = record.state
                            self._transition_window(session, record, "EMITTED", ref_id, "emit")
                            transitions.append(
                                {
                                    "window_id": record.window_id,
                                    "from_state": old_state,
                                    "to_state": "EMITTED",
                                    "reason": "emit",
                                }
                            )
                    else:
                        for record in rows_for("OPEN"):
                            old_state = record.state
                            self._transition_window(session, record, "CLOSED", ref_id, "close")
                            transitions.append(
                                {
                                    "window_id": record.window_id,
                                    "from_state": old_state,
                                    "to_state": "CLOSED",
                                    "reason": "close",
                                }
                            )

                    current_version = self._state_version(session)
                    if transitions:
                        current_version = self._next_state_version(session)
                        self._set_state_version(session, current_version)
                    self._finish_intent(session, intent_id, "COMPLETED")
                    response = {
                        "operation": "EMIT" if emit else "CLOSE",
                        "through_ms": request.through_ms,
                        "window_kind": request.window_kind,
                        "group_key": request.group_key,
                        "state_version": current_version,
                        "transition_count": len(transitions),
                        "transitions": transitions,
                    }
                return response
            except EngineError as exc:
                self._abort_intent(intent_id, exc)
                raise

    # ---------------------------------------------------------------- queries
    def query_windows(
        self,
        *,
        from_ms: int | None = None,
        to_ms: int | None = None,
        group_key: str | None = None,
        window_kind: str | None = None,
        state: str | None = None,
        quantiles: list[float] | None = None,
        include_type_history: bool = False,
    ) -> dict[str, Any]:
        from_value = -(2**63) if from_ms is None else from_ms
        to_value = 2**63 - 1 if to_ms is None else to_ms
        if from_value > to_value:
            raise EngineError(
                "E_INVALID_REQUEST",
                "from_ms must be <= to_ms",
                {"from_ms": from_value, "to_ms": to_value},
            )
        if state is not None and state not in ALL_STATES:
            raise EngineError(
                "E_INVALID_REQUEST",
                "state must be OPEN, CLOSED or EMITTED",
                {"state": state},
            )
        if window_kind is not None and window_kind not in {"tumbling", "sliding", "session"}:
            raise EngineError(
                "E_INVALID_WINDOW_SPEC",
                "window_kind must be tumbling, sliding or session",
                {"window_kind": window_kind},
            )
        quantiles = _normalize_quantiles(quantiles)

        with self.db.read_session() as session:
            state_version = self._state_version(session)
            stmt = select(WindowRecord).where(
                WindowRecord.end_ms >= from_value,
                WindowRecord.start_ms <= to_value,
            )
            if group_key is not None:
                stmt = stmt.where(WindowRecord.group_key == group_key)
            if window_kind is not None:
                stmt = stmt.where(WindowRecord.window_kind == window_kind)
            if state is not None:
                stmt = stmt.where(WindowRecord.state == state)
            records = list(
                session.scalars(
                    stmt.order_by(
                        WindowRecord.start_ms,
                        WindowRecord.end_ms,
                        WindowRecord.group_key,
                        WindowRecord.window_id,
                    )
                )
            )
            # The query range is inclusive. Window overlap follows the declared
            # window boundary: a right-open window ending exactly at from_ms
            # does not overlap the requested range.
            records = [
                record
                for record in records
                if record.start_ms <= to_value
                and (
                    record.end_ms > from_value
                    if record.boundary == "left_closed_right_open"
                    else record.end_ms >= from_value
                )
            ]
            groups = {record.group_key for record in records}
            group_types = {
                record.group_key: record
                for record in session.scalars(select(GroupTypeRecord).where(GroupTypeRecord.group_key.in_(groups)))
            } if groups else {}
            histories: dict[str, list[dict[str, Any]]] = {key: [] for key in groups}
            if include_type_history and groups:
                for evolution in session.scalars(
                    select(TypeEvolutionRecord)
                    .where(TypeEvolutionRecord.group_key.in_(groups))
                    .order_by(TypeEvolutionRecord.group_key, TypeEvolutionRecord.seq)
                ):
                    histories[evolution.group_key].append(_evolution_dict(evolution))

            windows = []
            for record in records:
                spec = spec_from_dict(_parse_json(record.spec_json, {}))
                aggregate = output_aggregate(_parse_json(record.aggregate_json), quantiles)
                group_type = group_types.get(record.group_key)
                payload = {
                    "window_id": record.window_id,
                    "window_kind": record.window_kind,
                    "spec_hash": record.spec_hash,
                    "start_ms": record.start_ms,
                    "end_ms": record.end_ms,
                    "group_key": record.group_key,
                    "state": record.state,
                    "boundary": spec.boundary_echo,
                    "version": record.version,
                    "created_batch": record.created_batch,
                    "updated_batch": record.updated_batch,
                    "last_event_ts": record.last_event_ts,
                    "group_current_type": (
                        {"kind": group_type.current_kind, "scale": group_type.current_scale}
                        if group_type is not None
                        else None
                    ),
                    **aggregate,
                }
                if include_type_history:
                    payload["type_evolution"] = histories.get(record.group_key, [])
                windows.append(payload)

            return {
                "state_version": state_version,
                "loaded_snapshot_version": int(self._meta(session, "loaded_snapshot_version", "0")),
                "query": {
                    "from_ms": from_value,
                    "to_ms": to_value,
                    "group_key": group_key,
                    "window_kind": window_kind,
                    "state": state,
                    "range_relation": "half_open_aware_overlap",
                },
                "window_count": len(windows),
                "windows": windows,
            }

    def query_events(
        self,
        *,
        from_ms: int | None = None,
        to_ms: int | None = None,
        group_key: str | None = None,
        batch_id: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        from_value = -(2**63) if from_ms is None else from_ms
        to_value = 2**63 - 1 if to_ms is None else to_ms
        limit = max(1, min(limit, 5000))
        with self.db.read_session() as session:
            stmt = select(BatchEventRecord).where(
                BatchEventRecord.timestamp_ms >= from_value,
                BatchEventRecord.timestamp_ms <= to_value,
            )
            if group_key is not None:
                stmt = stmt.where(BatchEventRecord.group_key == group_key)
            if batch_id is not None:
                stmt = stmt.where(BatchEventRecord.batch_id == batch_id)
            records = list(
                session.scalars(
                    stmt.order_by(BatchEventRecord.timestamp_ms, BatchEventRecord.batch_id, BatchEventRecord.event_id).limit(
                        limit
                    )
                )
            )
            state_version = self._state_version(session)
            return {
                "state_version": state_version,
                "event_count": len(records),
                "events": [
                    {
                        "batch_id": record.batch_id,
                        "event_id": record.event_id,
                        "timestamp_ms": record.timestamp_ms,
                        "group_key": record.group_key,
                        "measure": (
                            format_measure(int(record.measure_scaled), record.measure_scale)
                            if record.measure_scaled is not None
                            else None
                        ),
                        "measure_scaled": record.measure_scaled,
                        "measure_scale": record.measure_scale,
                        "measure_kind": record.measure_kind,
                        "labels": _parse_json(record.labels_json, {}),
                    }
                    for record in records
                ],
            }

    def query_late_events(self, *, batch_id: str | None = None, group_key: str | None = None) -> dict[str, Any]:
        with self.db.read_session() as session:
            stmt = select(LateEventRecord)
            if batch_id is not None:
                stmt = stmt.where(LateEventRecord.batch_id == batch_id)
            if group_key is not None:
                stmt = stmt.where(LateEventRecord.group_key == group_key)
            records = list(session.scalars(stmt.order_by(LateEventRecord.created_at, LateEventRecord.late_id)))
            return {
                "state_version": self._state_version(session),
                "late_event_count": len(records),
                "late_events": [
                    {
                        "late_id": record.late_id,
                        "batch_id": record.batch_id,
                        "event_id": record.event_id,
                        "timestamp_ms": record.timestamp_ms,
                        "group_key": record.group_key,
                        "measure": (
                            format_measure(int(record.measure_scaled), record.measure_scale)
                            if record.measure_scaled is not None
                            else None
                        ),
                        "labels": _parse_json(record.labels_json, {}),
                        "target_window_id": record.target_window_id,
                        "target_state": record.target_state,
                        "policy": record.policy,
                        "disposition": record.disposition,
                        "reason": record.reason,
                    }
                    for record in records
                ],
            }

    def type_evolution(self, *, group_key: str | None = None) -> dict[str, Any]:
        with self.db.read_session() as session:
            stmt = select(GroupTypeRecord)
            if group_key is not None:
                stmt = stmt.where(GroupTypeRecord.group_key == group_key)
            current_rows = list(session.scalars(stmt.order_by(GroupTypeRecord.group_key)))
            groups = [row.group_key for row in current_rows]
            histories: dict[str, list[dict[str, Any]]] = {key: [] for key in groups}
            if groups:
                evo_stmt = select(TypeEvolutionRecord).where(TypeEvolutionRecord.group_key.in_(groups))
                for evolution in session.scalars(evo_stmt.order_by(TypeEvolutionRecord.group_key, TypeEvolutionRecord.seq)):
                    histories[evolution.group_key].append(_evolution_dict(evolution))
            return {
                "state_version": self._state_version(session),
                "group_count": len(current_rows),
                "groups": [
                    {
                        "group_key": row.group_key,
                        "current_type": {"kind": row.current_kind, "scale": row.current_scale},
                        "last_batch_id": row.last_batch_id,
                        "evolution": histories.get(row.group_key, []),
                    }
                    for row in current_rows
                ],
            }

    def list_batches(self, limit: int = 100) -> dict[str, Any]:
        limit = max(1, min(limit, 1000))
        with self.db.read_session() as session:
            records = list(
                session.scalars(select(BatchRecord).order_by(BatchRecord.created_at.desc()).limit(limit))
            )
            return {
                "state_version": self._state_version(session),
                "batch_count": len(records),
                "batches": [
                    {
                        "batch_id": record.batch_id,
                        "status": record.status,
                        "request_hash": record.request_hash,
                        "reason": record.reason,
                        "created_at": _iso(record.created_at),
                        "finished_at": _iso(record.finished_at),
                    }
                    for record in records
                ],
            }

    def status(self) -> dict[str, Any]:
        with self.db.read_session() as session:
            window_counts = {
                state: int(
                    session.scalar(select(func.count()).select_from(WindowRecord).where(WindowRecord.state == state)) or 0
                )
                for state in sorted(ALL_STATES)
            }
            return {
                "database": self.db.url,
                "state_version": self._state_version(session),
                "snapshot_sequence": int(self._meta(session, "snapshot_sequence", "0")),
                "loaded_snapshot_version": int(self._meta(session, "loaded_snapshot_version", "0")),
                "batch_count": int(session.scalar(select(func.count()).select_from(BatchRecord)) or 0),
                "window_count": int(session.scalar(select(func.count()).select_from(WindowRecord)) or 0),
                "window_counts_by_state": window_counts,
                "late_event_count": int(session.scalar(select(func.count()).select_from(LateEventRecord)) or 0),
                "type_group_count": int(session.scalar(select(func.count()).select_from(GroupTypeRecord)) or 0),
                "pending_intent_count": int(
                    session.scalar(
                        select(func.count()).select_from(IntentRecord).where(IntentRecord.status == "PENDING")
                    )
                    or 0
                ),
                "latest_complete_snapshot_version": session.scalar(select(func.max(SnapshotRecord.version))),
                "boundary_semantics": {
                    "left_closed_right_open": {"start": "closed", "end": "open"},
                    "left_closed_right_closed": {"start": "closed", "end": "closed"},
                },
                "single_writer": True,
                "sqlite_wal": True,
            }

    # --------------------------------------------------------------- snapshots
    def create_snapshot(self, *, source: str = "local", crash_point: str | None = None) -> dict[str, Any]:
        with self._writer_lock:
            intent_id = self._write_intent("snapshot_create", source, {"source": source})
            self._maybe_crash("after_intent", crash_point)
            try:
                with self.db.write_session() as session:
                    max_stored = int(session.scalar(select(func.max(SnapshotRecord.version))) or 0)
                    sequence = int(self._meta(session, "snapshot_sequence", "0"))
                    version = max(max_stored, sequence) + 1
                    state = self._serialize_state(session)
                    snapshot: dict[str, Any] = {
                        "format": SNAPSHOT_FORMAT,
                        "version": version,
                        "created_at": utcnow().isoformat(),
                        "state": state,
                    }
                    checksum = _snapshot_checksum(snapshot)
                    snapshot["checksum"] = checksum
                    snapshot_json = _json(snapshot)
                    session.add(
                        SnapshotRecord(
                            version=version,
                            checksum=checksum,
                            snapshot_json=snapshot_json,
                            source=source,
                        )
                    )
                    self._set_meta(session, "snapshot_sequence", str(version))
                    self._finish_intent(session, intent_id, "COMPLETED")
                    session.flush()
                    self._maybe_crash("mid_transaction", crash_point)
                    metadata = {
                        "version": version,
                        "checksum": checksum,
                        "source": source,
                        "state_version": state["state_version"],
                        "created_at": snapshot["created_at"],
                    }
                return metadata
            except EngineError as exc:
                self._abort_intent(intent_id, exc)
                raise

    def list_snapshots(self) -> dict[str, Any]:
        with self.db.read_session() as session:
            records = list(session.scalars(select(SnapshotRecord).order_by(SnapshotRecord.version)))
            return {
                "state_version": self._state_version(session),
                "loaded_snapshot_version": int(self._meta(session, "loaded_snapshot_version", "0")),
                "snapshot_count": len(records),
                "snapshots": [
                    {
                        "version": record.version,
                        "checksum": record.checksum,
                        "source": record.source,
                        "created_at": _iso(record.created_at),
                    }
                    for record in records
                ],
            }

    def get_snapshot(self, version: int) -> dict[str, Any]:
        with self.db.read_session() as session:
            record = session.get(SnapshotRecord, version)
            if record is None:
                raise EngineError(
                    "E_NOT_FOUND",
                    "snapshot does not exist",
                    {"version": version},
                )
            return _parse_json(record.snapshot_json)

    def load_snapshot(self, snapshot: dict[str, Any], *, source: str = "imported") -> dict[str, Any]:
        validated = _validate_snapshot(snapshot)
        version = validated["version"]
        checksum = validated["checksum"]
        with self._writer_lock:
            with self.db.read_session() as session:
                loaded_version = int(self._meta(session, "loaded_snapshot_version", "0"))
            if version <= loaded_version:
                raise EngineError(
                    "E_SNAPSHOT_INVALID",
                    "snapshot version must increase monotonically",
                    {"incoming_version": version, "loaded_snapshot_version": loaded_version},
                )
            intent_id = self._write_intent(
                "snapshot_load",
                str(version),
                {"version": version, "checksum": checksum, "source": source},
            )
            try:
                with self.db.write_session() as session:
                    current_loaded = int(self._meta(session, "loaded_snapshot_version", "0"))
                    if version <= current_loaded:
                        raise EngineError(
                            "E_SNAPSHOT_INVALID",
                            "snapshot version must increase monotonically",
                            {"incoming_version": version, "loaded_snapshot_version": current_loaded},
                        )
                    existing = session.get(SnapshotRecord, version)
                    if existing is not None and existing.checksum != checksum:
                        raise EngineError(
                            "E_SNAPSHOT_INVALID",
                            "snapshot version already exists with a different checksum",
                            {"version": version, "existing_checksum": existing.checksum, "incoming_checksum": checksum},
                        )

                    self._replace_state_from_snapshot(session, validated)
                    previous_version = self._state_version(session)
                    next_version = max(previous_version + 1, int(validated["state"]["state_version"]) + 1)
                    self._set_state_version(session, next_version)
                    self._set_meta(session, "loaded_snapshot_version", str(version))
                    sequence = int(self._meta(session, "snapshot_sequence", "0"))
                    self._set_meta(session, "snapshot_sequence", str(max(sequence, version)))
                    if existing is None:
                        session.add(
                            SnapshotRecord(
                                version=version,
                                checksum=checksum,
                                snapshot_json=_json(snapshot),
                                source=source,
                            )
                        )
                    self._finish_intent(session, intent_id, "COMPLETED")
                    response = {
                        "status": "REPLACED",
                        "snapshot_version": version,
                        "checksum": checksum,
                        "previous_state_version": previous_version,
                        "state_version": next_version,
                        "window_count": len(validated["state"]["windows"]),
                        "late_event_count": len(validated["state"]["late_events"]),
                        "session_state_count": len(validated["state"]["sessions"]),
                        "type_evolution_count": len(validated["state"]["type_evolution"]),
                        "replacement": "all snapshot state tables replaced atomically",
                    }
                return response
            except EngineError as exc:
                self._abort_intent(intent_id, exc)
                raise
            except Exception as exc:
                self._abort_intent(
                    intent_id,
                    EngineError("E_SNAPSHOT_INVALID", "snapshot replacement failed", {"error": str(exc)}),
                )
                raise EngineError(
                    "E_SNAPSHOT_INVALID",
                    "snapshot replacement failed and was rolled back",
                    {"error": str(exc), "version": version},
                ) from exc

    def load_stored_snapshot(self, version: int) -> dict[str, Any]:
        return self.load_snapshot(self.get_snapshot(version), source="stored")

    def _serialize_state(self, session: Session) -> dict[str, Any]:
        windows = []
        for record in session.scalars(select(WindowRecord).order_by(WindowRecord.window_id)):
            windows.append(
                {
                    "window_id": record.window_id,
                    "spec_hash": record.spec_hash,
                    "window_kind": record.window_kind,
                    "boundary": record.boundary,
                    "spec": _parse_json(record.spec_json, {}),
                    "start_ms": record.start_ms,
                    "end_ms": record.end_ms,
                    "group_key": record.group_key,
                    "state": record.state,
                    "version": record.version,
                    "aggregate": _parse_json(record.aggregate_json, {}),
                    "created_batch": record.created_batch,
                    "updated_batch": record.updated_batch,
                    "last_event_ts": record.last_event_ts,
                    "created_at": _iso(record.created_at),
                    "updated_at": _iso(record.updated_at),
                }
            )
        late_events = [
            {
                "late_id": record.late_id,
                "batch_id": record.batch_id,
                "event_id": record.event_id,
                "timestamp_ms": record.timestamp_ms,
                "group_key": record.group_key,
                "measure_kind": record.measure_kind,
                "measure_scale": record.measure_scale,
                "measure_scaled": record.measure_scaled,
                "labels": _parse_json(record.labels_json, {}),
                "target_window_id": record.target_window_id,
                "target_state": record.target_state,
                "policy": record.policy,
                "disposition": record.disposition,
                "reason": record.reason,
                "created_at": _iso(record.created_at),
            }
            for record in session.scalars(select(LateEventRecord).order_by(LateEventRecord.late_id))
        ]
        sessions = [
            {
                "spec_hash": record.spec_hash,
                "group_key": record.group_key,
                "window_id": record.window_id,
                "start_ms": record.start_ms,
                "end_ms": record.end_ms,
                "last_event_ts": record.last_event_ts,
                "batch_id": record.batch_id,
                "updated_at": _iso(record.updated_at),
            }
            for record in session.scalars(select(SessionStateRecord).order_by(SessionStateRecord.group_key))
        ]
        type_evolution = [
            _evolution_snapshot_dict(record)
            for record in session.scalars(
                select(TypeEvolutionRecord).order_by(TypeEvolutionRecord.group_key, TypeEvolutionRecord.seq)
            )
        ]
        group_types = [
            {
                "group_key": record.group_key,
                "current_kind": record.current_kind,
                "current_scale": record.current_scale,
                "last_batch_id": record.last_batch_id,
                "updated_at": _iso(record.updated_at),
            }
            for record in session.scalars(select(GroupTypeRecord).order_by(GroupTypeRecord.group_key))
        ]
        batches = [
            {
                "batch_id": record.batch_id,
                "request_hash": record.request_hash,
                "status": record.status,
                "response": _parse_json(record.response_json),
                "reason": record.reason,
                "created_at": _iso(record.created_at),
                "finished_at": _iso(record.finished_at),
            }
            for record in session.scalars(select(BatchRecord).order_by(BatchRecord.batch_id))
        ]
        batch_events = [
            {
                "batch_id": record.batch_id,
                "event_id": record.event_id,
                "sequence_no": record.sequence_no,
                "timestamp_ms": record.timestamp_ms,
                "group_key": record.group_key,
                "measure_kind": record.measure_kind,
                "measure_scale": record.measure_scale,
                "measure_scaled": record.measure_scaled,
                "has_value": record.has_value,
                "labels": _parse_json(record.labels_json, {}),
                "created_at": _iso(record.created_at),
            }
            for record in session.scalars(
                select(BatchEventRecord).order_by(BatchEventRecord.timestamp_ms, BatchEventRecord.batch_id, BatchEventRecord.event_id)
            )
        ]
        transitions = [
            {
                "transition_id": record.transition_id,
                "window_id": record.window_id,
                "from_state": record.from_state,
                "to_state": record.to_state,
                "reference_id": record.reference_id,
                "reason": record.reason,
                "created_at": _iso(record.created_at),
            }
            for record in session.scalars(
                select(WindowTransitionRecord).order_by(
                    WindowTransitionRecord.created_at, WindowTransitionRecord.transition_id
                )
            )
        ]
        return {
            "state_version": self._state_version(session),
            "windows": windows,
            "late_events": late_events,
            "sessions": sessions,
            "type_evolution": type_evolution,
            "group_types": group_types,
            "batches": batches,
            "batch_events": batch_events,
            "window_transitions": transitions,
        }

    def _replace_state_from_snapshot(self, session: Session, validated: dict[str, Any]) -> None:
        state = validated["state"]
        for model in (
            WindowTransitionRecord,
            LateEventRecord,
            SessionStateRecord,
            TypeEvolutionRecord,
            GroupTypeRecord,
            WindowRecord,
            BatchEventRecord,
            BatchRecord,
        ):
            session.execute(delete(model))

        for item in state["windows"]:
            session.add(_window_from_snapshot(item))
        for item in state["late_events"]:
            session.add(_late_from_snapshot(item))
        for item in state["sessions"]:
            session.add(
                SessionStateRecord(
                    spec_hash=item["spec_hash"],
                    group_key=item["group_key"],
                    window_id=item["window_id"],
                    start_ms=int(item["start_ms"]),
                    end_ms=int(item["end_ms"]),
                    last_event_ts=int(item["last_event_ts"]),
                    batch_id=item["batch_id"],
                    updated_at=_dt(item.get("updated_at")) or utcnow(),
                )
            )
        for item in state["type_evolution"]:
            session.add(_evolution_from_snapshot(item))
        for item in state["group_types"]:
            session.add(
                GroupTypeRecord(
                    group_key=item["group_key"],
                    current_kind=item["current_kind"],
                    current_scale=int(item["current_scale"]),
                    last_batch_id=item["last_batch_id"],
                    updated_at=_dt(item.get("updated_at")) or utcnow(),
                )
            )
        for item in state["batches"]:
            session.add(
                BatchRecord(
                    batch_id=item["batch_id"],
                    request_hash=item["request_hash"],
                    status=item["status"],
                    response_json=_json(item.get("response")),
                    reason=item.get("reason"),
                    created_at=_dt(item.get("created_at")) or utcnow(),
                    finished_at=_dt(item.get("finished_at")),
                )
            )
        session.flush()
        for item in state["batch_events"]:
            session.add(
                BatchEventRecord(
                    batch_id=item["batch_id"],
                    event_id=item["event_id"],
                    sequence_no=int(item["sequence_no"]),
                    timestamp_ms=int(item["timestamp_ms"]),
                    group_key=item["group_key"],
                    measure_kind=item["measure_kind"],
                    measure_scale=int(item["measure_scale"]),
                    measure_scaled=item["measure_scaled"],
                    has_value=bool(item["has_value"]),
                    labels_json=_json(item.get("labels", {})),
                    created_at=_dt(item.get("created_at")) or utcnow(),
                )
            )
        for item in state["window_transitions"]:
            session.add(
                WindowTransitionRecord(
                    transition_id=item["transition_id"],
                    window_id=item["window_id"],
                    from_state=item["from_state"],
                    to_state=item["to_state"],
                    reference_id=item["reference_id"],
                    reason=item["reason"],
                    created_at=_dt(item.get("created_at")) or utcnow(),
                )
            )
        session.flush()

    # --------------------------------------------------------------- recovery
    def recover_pending_intents(self, *, reason: str = "startup", persist: bool = True) -> dict[str, Any]:
        with self._writer_lock:
            with self.db.write_session() as session:
                pending = list(
                    session.scalars(
                        select(IntentRecord)
                        .where(IntentRecord.status == "PENDING")
                        .order_by(IntentRecord.created_at, IntentRecord.intent_id)
                    )
                )
                actions: list[dict[str, Any]] = []
                aborted_batches: list[dict[str, Any]] = []
                for intent in pending:
                    payload = _parse_json(intent.payload_json, {})
                    if intent.kind == "batch":
                        batch = session.get(BatchRecord, intent.ref_id)
                        if batch is not None and batch.status == "COMMITTED":
                            self._finish_intent(session, intent.intent_id, "COMPLETED")
                            actions.append(_recovery_action(intent, "intent_reconciled_with_committed_batch"))
                        else:
                            if batch is None:
                                batch = BatchRecord(
                                    batch_id=intent.ref_id,
                                    request_hash=payload.get("request_hash", "unknown"),
                                    status="ABORTED",
                                    response_json=None,
                                    reason="recovered_pending_intent",
                                    created_at=intent.created_at,
                                    finished_at=utcnow(),
                                )
                                session.add(batch)
                            else:
                                batch.status = "ABORTED"
                                batch.reason = "recovered_pending_intent"
                                batch.finished_at = utcnow()
                            self._finish_intent(
                                session,
                                intent.intent_id,
                                "ABORTED",
                                {"recovered": True, "action": "batch_marked_aborted"},
                            )
                            actions.append(_recovery_action(intent, "batch_marked_aborted"))
                            aborted_batches.append(
                                {
                                    "batch_id": intent.ref_id,
                                    "request_hash": payload.get("request_hash"),
                                    "reason": "recovered_pending_intent",
                                }
                            )
                    elif intent.kind in {"snapshot_create", "snapshot_load"}:
                        raw_version = payload.get("version")
                        version = int(raw_version) if raw_version is not None else 0
                        snapshot = session.get(SnapshotRecord, version) if version else None
                        if snapshot is not None and intent.kind == "snapshot_load":
                            self._finish_intent(session, intent.intent_id, "COMPLETED")
                            actions.append(_recovery_action(intent, "intent_reconciled_with_committed_snapshot_load"))
                        else:
                            self._finish_intent(
                                session,
                                intent.intent_id,
                                "ABORTED",
                                {"recovered": True, "action": "snapshot_intent_aborted"},
                            )
                            actions.append(_recovery_action(intent, "snapshot_intent_aborted"))
                    else:
                        self._finish_intent(
                            session,
                            intent.intent_id,
                            "ABORTED",
                            {"recovered": True, "action": "intent_aborted_atomic_transaction_was_not_committed"},
                        )
                        actions.append(_recovery_action(intent, "intent_aborted_atomic_transaction_was_not_committed"))

                state_version = self._state_version(session)
                report = {
                    "status": "RECOVERED" if pending else "NO_PENDING_INTENT",
                    "reason": reason,
                    "recovered_at": utcnow().isoformat(),
                    "pending_intent_count": len(pending),
                    "action_count": len(actions),
                    "actions": actions,
                    "aborted_batches": aborted_batches,
                    "state_version": state_version,
                    "loaded_snapshot_version": int(self._meta(session, "loaded_snapshot_version", "0")),
                    "latest_complete_snapshot_version": session.scalar(select(func.max(SnapshotRecord.version))),
                    "durable_facts_only": True,
                    "transaction_rule": "atomic SQLite commit; a pending intent without its terminal row is aborted",
                }
                if pending and persist:
                    session.add(RecoveryReportRecord(report_json=_json(report)))
            self.last_recovery_report = report
            return report

    def latest_recovery_report(self) -> dict[str, Any]:
        with self.db.read_session() as session:
            record = session.scalar(
                select(RecoveryReportRecord).order_by(RecoveryReportRecord.id.desc()).limit(1)
            )
            if record is None:
                return {
                    "status": "NO_RECOVERY_REPORT",
                    "message": "no pending intent has required recovery in this database",
                    "state_version": self._state_version(session),
                    "latest_complete_snapshot_version": session.scalar(select(func.max(SnapshotRecord.version))),
                }
            return _parse_json(record.report_json)


# ------------------------------------------------------------------ helpers
def _normalize_quantiles(values: Iterable[float] | None) -> list[float] | None:
    if values is None:
        return None
    result = sorted(set(float(value) for value in values))
    for value in result:
        if not 0 <= value <= 1:
            raise EngineError(
                "E_INVALID_REQUEST",
                "quantiles must be between 0 and 1",
                {"quantile": value},
            )
    return result or None


def _evolution_dict(record: TypeEvolutionRecord) -> dict[str, Any]:
    return {
        "sequence": record.seq,
        "from": None if record.from_kind is None else {"kind": record.from_kind, "scale": record.from_scale},
        "to": {"kind": record.to_kind, "scale": record.to_scale},
        "batch_id": record.batch_id,
        "reason": record.reason,
        "rounding": record.rounding,
        "basis": record.basis,
    }


def _evolution_snapshot_dict(record: TypeEvolutionRecord) -> dict[str, Any]:
    return {
        "group_key": record.group_key,
        "seq": record.seq,
        "from_kind": record.from_kind,
        "from_scale": record.from_scale,
        "to_kind": record.to_kind,
        "to_scale": record.to_scale,
        "batch_id": record.batch_id,
        "reason": record.reason,
        "rounding": record.rounding,
        "basis": record.basis,
        "created_at": _iso(record.created_at),
    }


def _evolution_from_snapshot(item: dict[str, Any]) -> TypeEvolutionRecord:
    return TypeEvolutionRecord(
        group_key=item["group_key"],
        seq=int(item["seq"]),
        from_kind=item.get("from_kind"),
        from_scale=item.get("from_scale"),
        to_kind=item["to_kind"],
        to_scale=int(item["to_scale"]),
        batch_id=item["batch_id"],
        reason=item["reason"],
        rounding=item["rounding"],
        basis=item["basis"],
        created_at=_dt(item.get("created_at")) or utcnow(),
    )


def _window_from_snapshot(item: dict[str, Any]) -> WindowRecord:
    return WindowRecord(
        window_id=item["window_id"],
        spec_hash=item["spec_hash"],
        window_kind=item["window_kind"],
        boundary=item["boundary"],
        spec_json=_json(item.get("spec", {})),
        start_ms=int(item["start_ms"]),
        end_ms=int(item["end_ms"]),
        group_key=item["group_key"],
        state=item["state"],
        version=int(item["version"]),
        aggregate_json=_json(item["aggregate"]),
        created_batch=item["created_batch"],
        updated_batch=item.get("updated_batch"),
        last_event_ts=item.get("last_event_ts"),
        created_at=_dt(item.get("created_at")) or utcnow(),
        updated_at=_dt(item.get("updated_at")) or utcnow(),
    )


def _late_from_snapshot(item: dict[str, Any]) -> LateEventRecord:
    return LateEventRecord(
        late_id=item["late_id"],
        batch_id=item["batch_id"],
        event_id=item["event_id"],
        timestamp_ms=int(item["timestamp_ms"]),
        group_key=item["group_key"],
        measure_kind=item["measure_kind"],
        measure_scale=int(item["measure_scale"]),
        measure_scaled=item.get("measure_scaled"),
        labels_json=_json(item.get("labels", {})),
        target_window_id=item["target_window_id"],
        target_state=item["target_state"],
        policy=item["policy"],
        disposition=item["disposition"],
        reason=item["reason"],
        created_at=_dt(item.get("created_at")) or utcnow(),
    )


def _snapshot_checksum(snapshot: dict[str, Any]) -> str:
    body = {key: value for key, value in snapshot.items() if key != "checksum"}
    return _sha256(body)


def _validate_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        raise EngineError("E_SNAPSHOT_INVALID", "snapshot must be a JSON object", {})
    if snapshot.get("format") != SNAPSHOT_FORMAT:
        raise EngineError(
            "E_SNAPSHOT_INVALID",
            "unsupported snapshot format",
            {"expected": SNAPSHOT_FORMAT, "actual": snapshot.get("format")},
        )
    version = snapshot.get("version")
    if type(version) is not int or version <= 0:
        raise EngineError("E_SNAPSHOT_INVALID", "snapshot version must be a positive integer", {"version": version})
    checksum = snapshot.get("checksum")
    if not isinstance(checksum, str):
        raise EngineError("E_SNAPSHOT_INVALID", "snapshot checksum is missing", {"version": version})
    computed = _snapshot_checksum(snapshot)
    if checksum != computed:
        raise EngineError(
            "E_SNAPSHOT_INVALID",
            "snapshot checksum verification failed",
            {"version": version, "declared_checksum": checksum, "computed_checksum": computed},
        )
    state = snapshot.get("state")
    if not isinstance(state, dict):
        raise EngineError("E_SNAPSHOT_INVALID", "snapshot state is missing", {"version": version})
    required_lists = [
        "windows",
        "late_events",
        "sessions",
        "type_evolution",
        "group_types",
        "batches",
        "batch_events",
        "window_transitions",
    ]
    for key in required_lists:
        if not isinstance(state.get(key), list):
            raise EngineError(
                "E_SNAPSHOT_INVALID",
                f"snapshot state.{key} must be a list",
                {"version": version, "field": key},
            )
    if type(state.get("state_version")) is not int or state["state_version"] < 0:
        raise EngineError(
            "E_SNAPSHOT_INVALID",
            "snapshot state_version must be a non-negative integer",
            {"version": version, "state_version": state.get("state_version")},
        )
    # Validate discriminators before any database mutation.
    for window in state["windows"]:
        if not isinstance(window, dict) or window.get("state") not in ALL_STATES:
            raise EngineError(
                "E_SNAPSHOT_INVALID",
                "snapshot contains an invalid window state",
                {"version": version, "window": window},
            )
        if not isinstance(window.get("aggregate"), dict):
            raise EngineError(
                "E_SNAPSHOT_INVALID",
                "snapshot window aggregate must be an object",
                {"version": version, "window_id": window.get("window_id")},
            )
    for batch in state["batches"]:
        if not isinstance(batch, dict) or batch.get("status") not in {"COMMITTED", "ABORTED"}:
            raise EngineError(
                "E_SNAPSHOT_INVALID",
                "snapshot contains an invalid batch status",
                {"version": version, "batch": batch},
            )
    return {"version": version, "checksum": checksum, "state": state, "snapshot": snapshot}


def _recovery_action(intent: IntentRecord, action: str) -> dict[str, Any]:
    return {
        "intent_id": intent.intent_id,
        "kind": intent.kind,
        "ref_id": intent.ref_id,
        "action": action,
        "intent_created_at": _iso(intent.created_at),
    }
