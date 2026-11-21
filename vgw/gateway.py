from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any

from .codec import (
    DecodedResponse,
    decode_request,
    decode_response,
    encode_request,
    parse_sequence,
)
from .errors import GatewayError
from .models import FrameSubmission, model_dump_canonical
from .snapshot import (
    SnapshotDocument,
    apply_patch,
    binding_endpoint,
    validate_patch,
    validate_snapshot,
)
from .store import Store, utc_now
from .transport import udp_exchange


@dataclass(slots=True)
class Outcome:
    signature: str
    ok: bool
    result: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None
    error_context: dict[str, Any] | None = None


@dataclass(slots=True)
class Operation:
    frame_id: str
    session_id: str
    sequence: int
    payload: bytes
    payload_hash: str
    signature: str
    timeout_ms: int
    binding_priority: int
    binding_channel: int
    egress_label: str
    egress_address: str
    snapshot_version: int
    max_payload: int
    frame_bytes: bytes
    arrival_index: int
    enqueued_at: str
    future: asyncio.Future[Outcome] = field(default_factory=lambda: asyncio.get_running_loop().create_future())
    intent_id: str | None = None
    state: str = "queued"
    started_at: str | None = None
    task: asyncio.Task[Any] | None = None


@dataclass(slots=True)
class ChannelSlot:
    channel_id: int
    operation: Operation | None = None
    acquired_at: str | None = None


class Gateway:
    """Serialized writer for leases/rebuilds; immutable commits for concurrent readers."""

    def __init__(self, store: Store, *, completed_cache_size: int = 2048):
        self.store = store
        self._writer_lock = asyncio.Lock()
        self._rebuild_lock = asyncio.Lock()
        self._scheduler_event = asyncio.Event()
        self._unblock_event = asyncio.Event()
        self._unblock_event.set()
        self._work_changed = asyncio.Event()
        self._scheduler_task: asyncio.Task[Any] | None = None
        self._running = False
        self._snapshot: SnapshotDocument | None = None
        self._channels: list[ChannelSlot] = []
        self._queue: list[Operation] = []
        self._active_by_session: dict[str, Operation] = {}
        self._operations: dict[str, Operation] = {}
        self._completed: OrderedDict[str, Outcome] = OrderedDict()
        self._completed_cache_size = completed_cache_size
        self._blocked_sessions: set[str] = set()
        self._last_acked: dict[str, int] = {}
        self._next_expected: dict[str, int] = {}
        self._leases: dict[str, dict[str, Any]] = {}
        self._stats: dict[str, dict[str, Any]] = {}
        self._sessions: dict[str, dict[str, Any]] = {}
        self._arrival_counter = 0
        self._recovery_report: dict[str, Any] | None = None
        self._published: dict[str, Any] = {}
        self._round_counter = 0
        self._last_round: dict[str, Any] | None = None
        self._started_at: str | None = None
        self._stopping = False

    async def start(self) -> dict[str, Any]:
        self._recovery_report = self.store.recover_incomplete_intents()
        latest = self.store.latest_snapshot()
        if latest is not None:
            self._snapshot = validate_snapshot(latest[1])
        self._sessions = self.store.load_sessions()
        self._leases = self.store.load_leases()
        self._stats = self.store.load_stats()
        self._last_acked = {
            session_id: int(row["last_acked_seq"])
            for session_id, row in self._sessions.items()
        }
        self._next_expected = {
            session_id: int(row["last_acked_seq"]) + 1
            for session_id, row in self._sessions.items()
        }
        if self._snapshot is not None:
            self._channels = [
                ChannelSlot(channel_id=index)
                for index in range(self._snapshot.channel_count)
            ]
        self._running = True
        self._stopping = False
        self._started_at = utc_now()
        self._publish()
        self._scheduler_task = asyncio.create_task(
            self._scheduler_loop(), name="vgw-scheduler"
        )
        return self._recovery_report

    async def stop(self) -> None:
        self._running = False
        self._stopping = True
        self._scheduler_event.set()
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            await asyncio.gather(self._scheduler_task, return_exceptions=True)
            self._scheduler_task = None
        active_tasks = [
            operation.task
            for operation in self._active_by_session.values()
            if operation.task is not None
        ]
        for task in active_tasks:
            task.cancel()
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)

    # ------------------------------ mutations -----------------------------
    async def load_snapshot(self, raw: dict[str, Any]) -> dict[str, Any]:
        # Full validation happens before touching any state.
        candidate = validate_snapshot(raw)
        async with self._rebuild_lock:
            async with self._writer_lock:
                if self._snapshot is not None and candidate.version <= self._snapshot.version:
                    raise GatewayError(
                        "E_SNAPSHOT_INVALID",
                        "snapshot version is not monotonically increasing",
                        {
                            "current_version": self._snapshot.version,
                            "incoming_version": candidate.version,
                        },
                    )
                old_sessions = set(self._snapshot.bindings) if self._snapshot else set()
                new_sessions = set(candidate.bindings)
                affected = tuple(sorted(old_sessions | new_sessions))
                self._block_sessions(affected)
            try:
                await self._wait_sessions_idle(affected)
                async with self._writer_lock:
                    stats = self.store.commit_snapshot(
                        candidate,
                        model_dump_canonical(candidate),
                        affected_sessions=affected,
                        trace_kind="snapshot_loaded",
                        trace_detail={
                            "version": candidate.version,
                            "digest": candidate.digest,
                            "affected_sessions": list(affected),
                            "old_version": self._snapshot.version if self._snapshot else None,
                        },
                    )
                    self._snapshot = candidate
                    self._stats = stats
                    self._leases = self.store.load_leases()
                    self._sessions = self.store.load_sessions()
                    self._channels = [
                        ChannelSlot(channel_id=index)
                        for index in range(candidate.channel_count)
                    ]
                    for session_id in candidate.bindings:
                        self._last_acked.setdefault(session_id, 0)
                        self._next_expected.setdefault(session_id, 1)
                    self._publish()
                return {
                    "accepted": True,
                    "version": candidate.version,
                    "digest": candidate.digest,
                    "affected_sessions": list(affected),
                    "old_version": None,
                    "atomic": True,
                }
            finally:
                async with self._writer_lock:
                    self._unblock_sessions(affected)

    async def apply_patch(self, raw: dict[str, Any]) -> dict[str, Any]:
        patch = validate_patch(raw)
        async with self._rebuild_lock:
            async with self._writer_lock:
                if self._snapshot is None:
                    raise GatewayError(
                        "E_SNAPSHOT_INVALID",
                        "no active snapshot exists",
                        {"base_version": patch.base_version},
                    )
                if patch.base_version != self._snapshot.version:
                    raise GatewayError(
                        "E_STALE_BASE",
                        "patch base version is not the current snapshot version",
                        {
                            "base_version": patch.base_version,
                            "current_version": self._snapshot.version,
                        },
                    )
                application = apply_patch(self._snapshot, patch)
                affected = application.affected_sessions
                old_version = self._snapshot.version
                self._block_sessions(affected)
            try:
                await self._wait_sessions_idle(affected)
                async with self._writer_lock:
                    if self._snapshot is None or self._snapshot.version != patch.base_version:
                        raise GatewayError(
                            "E_STALE_BASE",
                            "snapshot changed while the patch was waiting for quiescence",
                            {
                                "base_version": patch.base_version,
                                "current_version": self._snapshot.version
                                if self._snapshot
                                else None,
                            },
                        )
                    stats = self.store.commit_snapshot(
                        application.snapshot,
                        model_dump_canonical(application.snapshot),
                        affected_sessions=affected,
                        trace_kind="route_patch_committed",
                        trace_detail={
                            "base_version": patch.base_version,
                            "version": application.snapshot.version,
                            "affected_sessions": list(affected),
                            "changed_bindings": list(application.changed_bindings),
                            "changed_endpoints": list(application.changed_endpoints),
                            "rebuild_transaction": "snapshot+stats+lease_context",
                        },
                    )
                    self._snapshot = application.snapshot
                    self._stats = stats
                    self._leases = self.store.load_leases()
                    self._publish()
                rebuilt_stats = {
                    session_id: self._stats.get(session_id)
                    for session_id in affected
                }
                return {
                    "accepted": True,
                    "base_version": old_version,
                    "version": application.snapshot.version,
                    "digest": application.snapshot.digest,
                    "affected_sessions": list(affected),
                    "changed_bindings": list(application.changed_bindings),
                    "changed_endpoints": list(application.changed_endpoints),
                    "rebuilt_stats": rebuilt_stats,
                    "atomic": True,
                    "quiesced_before_switch": True,
                }
            finally:
                async with self._writer_lock:
                    self._unblock_sessions(affected)

    async def submit_frame(self, submission: FrameSubmission | dict[str, Any]) -> dict[str, Any]:
        if isinstance(submission, dict):
            submission = FrameSubmission.model_validate(submission)
        sequence = parse_sequence(submission.sequence)
        payload = self._materialize_payload(submission)
        payload_hash = "sha256:" + hashlib.sha256(payload).hexdigest()
        signature = hashlib.sha256(
            (
                submission.session_id
                + "\x00"
                + str(sequence)
                + "\x00"
                + payload_hash
            ).encode("utf-8")
        ).hexdigest()

        while True:
            wait_event: asyncio.Event | None = None
            duplicate: Operation | None = None
            completed: Outcome | None = None
            async with self._writer_lock:
                duplicate = self._operations.get(submission.frame_id)
                if duplicate is not None:
                    if duplicate.signature != signature:
                        raise GatewayError(
                            "E_CONFLICTING_FRAME",
                            "frame_id was already accepted with different content",
                            {
                                "frame_id": submission.frame_id,
                                "existing_signature": duplicate.signature,
                                "incoming_signature": signature,
                            },
                        )
                else:
                    completed = self._completed.get(submission.frame_id)
                    if completed is None:
                        completed = self._outcome_from_persisted_frame(
                            submission.frame_id, signature
                        )
                    if completed is not None:
                        if completed.signature != signature:
                            raise GatewayError(
                                "E_CONFLICTING_FRAME",
                                "frame_id was already finalized with different content",
                                {
                                    "frame_id": submission.frame_id,
                                    "existing_signature": completed.signature,
                                    "incoming_signature": signature,
                                },
                            )
                    elif submission.session_id in self._blocked_sessions:
                        wait_event = self._unblock_event
                    else:
                        duplicate = self._admit_locked(
                            submission,
                            sequence=sequence,
                            payload=payload,
                            payload_hash=payload_hash,
                            signature=signature,
                        )
            if wait_event is not None:
                await wait_event.wait()
                continue
            if completed is not None:
                return self._raise_or_result(completed)
            if duplicate is not None:
                outcome = await asyncio.shield(duplicate.future)
                return self._raise_or_result(outcome)
            raise RuntimeError("unreachable frame admission state")

    def _admit_locked(
        self,
        submission: FrameSubmission,
        *,
        sequence: int,
        payload: bytes,
        payload_hash: str,
        signature: str,
    ) -> Operation:
        if self._snapshot is None:
            raise GatewayError(
                "E_SNAPSHOT_INVALID",
                "no active snapshot is loaded",
                {"session_id": submission.session_id},
            )
        binding = self._snapshot.bindings.get(submission.session_id)
        if binding is None:
            raise GatewayError(
                "E_FRAME_INVALID",
                "session is not bound in the active snapshot",
                {
                    "session_id": submission.session_id,
                    "snapshot_version": self._snapshot.version,
                },
            )
        expected = self._next_expected.get(
            submission.session_id,
            self._last_acked.get(submission.session_id, 0) + 1,
        )
        if sequence != expected:
            raise GatewayError(
                "E_FRAME_INVALID",
                "session sequence must be contiguous with no gaps",
                {
                    "session_id": submission.session_id,
                    "expected_sequence": expected,
                    "received_sequence": sequence,
                },
            )
        if len(self._queue) >= self._snapshot.queue_capacity:
            raise GatewayError(
                "E_CHANNEL_QUEUE_FULL",
                "the bounded waiting queue is full",
                {
                    "queue_depth": len(self._queue),
                    "queue_capacity": self._snapshot.queue_capacity,
                    "active_sessions": sorted(self._active_by_session),
                },
            )
        host, port, egress_address = binding_endpoint(
            self._snapshot, submission.session_id
        )
        frame_bytes = encode_request(
            submission.session_id,
            binding.channel,
            sequence,
            payload,
            max_payload=binding.max_payload,
        )
        timeout_ms = submission.timeout_ms or self._snapshot.default_timeout_ms
        self._arrival_counter += 1
        operation = Operation(
            frame_id=submission.frame_id,
            session_id=submission.session_id,
            sequence=sequence,
            payload=payload,
            payload_hash=payload_hash,
            signature=signature,
            timeout_ms=timeout_ms,
            binding_priority=binding.priority,
            binding_channel=binding.channel,
            egress_label=binding.egress,
            egress_address=egress_address,
            snapshot_version=self._snapshot.version,
            max_payload=binding.max_payload,
            frame_bytes=frame_bytes,
            arrival_index=self._arrival_counter,
            enqueued_at=utc_now(),
        )
        self.store.commit_queued_frame(
            frame_id=operation.frame_id,
            session_id=operation.session_id,
            sequence=operation.sequence,
            payload_hash=operation.payload_hash,
            payload_len=len(operation.payload),
            snapshot_version=operation.snapshot_version,
        )
        self._operations[operation.frame_id] = operation
        self._queue.append(operation)
        self._next_expected[operation.session_id] = sequence + 1
        self._signal_work_changed()
        self._scheduler_event.set()
        self._publish()
        # Start a scheduling round in the same serialized admission boundary. This
        # prevents a burst of concurrent HTTP submissions from being mistaken for
        # queue saturation before any channel has had a chance to accept a frame.
        self._dispatch_round_locked()
        return operation

    # ----------------------------- scheduler ------------------------------
    async def _scheduler_loop(self) -> None:
        while self._running:
            await self._scheduler_event.wait()
            while self._running:
                self._scheduler_event.clear()
                async with self._writer_lock:
                    more_possible = self._dispatch_round_locked()
                if not more_possible:
                    break
                await asyncio.sleep(0.001)

    def _dispatch_round_locked(self) -> bool:
        if self._snapshot is None:
            return False
        self._round_counter += 1
        round_started_perf = time.perf_counter()
        budget_seconds = self._snapshot.round_budget_ms / 1000
        dispatched: list[dict[str, Any]] = []
        while True:
            elapsed = time.perf_counter() - round_started_perf
            if elapsed >= budget_seconds:
                break
            free_channels = [slot for slot in self._channels if slot.operation is None]
            if not free_channels:
                break
            free_ids = {slot.channel_id for slot in free_channels}
            operation = self._select_next_locked(free_ids)
            if operation is None:
                break
            slot = next(item for item in free_channels if item.channel_id == operation.binding_channel)
            intent_id = str(uuid.uuid4())
            self.store.commit_dispatch(
                intent_id=intent_id,
                frame_id=operation.frame_id,
                session_id=operation.session_id,
                sequence=operation.sequence,
                channel_id=slot.channel_id,
                egress=operation.egress_address,
                snapshot_version=operation.snapshot_version,
                payload_len=len(operation.payload),
            )
            self._queue.remove(operation)
            operation.intent_id = intent_id
            operation.state = "active"
            operation.started_at = utc_now()
            slot.operation = operation
            slot.acquired_at = operation.started_at
            self._active_by_session[operation.session_id] = operation
            operation.task = asyncio.create_task(
                self._run_operation(operation, slot),
                name=f"vgw-frame-{operation.frame_id}",
            )
            dispatched.append(
                {
                    "frame_id": operation.frame_id,
                    "session_id": operation.session_id,
                    "channel_id": slot.channel_id,
                }
            )
        if dispatched:
            self._last_round = {
                "round": self._round_counter,
                "round_budget_ms": self._snapshot.round_budget_ms,
                "elapsed_ms": round((time.perf_counter() - round_started_perf) * 1000, 3),
                "dispatched": dispatched,
            }
            self._publish()
            self._signal_work_changed()
        if not self._queue:
            return False
        free_channel_ids = {
            slot.channel_id for slot in self._channels if slot.operation is None
        }
        return self._select_next_locked(free_channel_ids) is not None

    def _select_next_locked(self, free_channel_ids: set[int]) -> Operation | None:
        candidates = [
            operation
            for operation in self._queue
            if operation.session_id not in self._active_by_session
            and operation.binding_channel in free_channel_ids
        ]
        if not candidates:
            return None
        return min(
            candidates,
            key=lambda item: (-item.binding_priority, item.arrival_index),
        )

    async def _run_operation(self, operation: Operation, slot: ChannelSlot) -> None:
        try:
            host, port = _split_address(operation.egress_address)
            response_raw, transport_meta = await udp_exchange(
                (host, port), operation.frame_bytes, operation.timeout_ms
            )
            response = decode_response(response_raw)
            if response.status != 0:
                payload = response.payload_json() or {}
                raise GatewayError(
                    "E_FRAME_INVALID",
                    "egress decapsulator rejected the frame",
                    {
                        "phase": "egress_response",
                        "status": response.status,
                        "remote_code": payload.get("code") if isinstance(payload, dict) else None,
                        "remote_message": payload.get("message")
                        if isinstance(payload, dict)
                        else None,
                    },
                )
            if (
                response.session_id != operation.session_id
                or response.sequence != operation.sequence
                or response.channel != operation.binding_channel
            ):
                raise GatewayError(
                    "E_FRAME_INVALID",
                    "response identity does not match the dispatched frame",
                    {
                        "phase": "response_identity",
                        "expected": {
                            "session_id": operation.session_id,
                            "sequence": operation.sequence,
                            "channel": operation.binding_channel,
                        },
                        "received": {
                            "session_id": response.session_id,
                            "sequence": response.sequence,
                            "channel": response.channel,
                        },
                    },
                )
            response_payload = response.payload_json()
            if response_payload is None:
                response_payload = {
                    "raw_payload_b64": base64.b64encode(response.payload).decode("ascii")
                }
            result = self._success_result(
                operation, slot, response, response_payload, transport_meta
            )
            await self._finalize_success(operation, slot, result)
        except GatewayError as exc:
            await self._finalize_failure(operation, slot, exc)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._finalize_failure(
                operation,
                slot,
                GatewayError(
                    "E_INTERNAL",
                    "unexpected frame processing error",
                    {"exception": type(exc).__name__, "detail": str(exc)},
                ),
            )

    async def _finalize_success(
        self,
        operation: Operation,
        slot: ChannelSlot,
        result: dict[str, Any],
    ) -> None:
        if operation.intent_id is None:
            raise RuntimeError("active operation has no dispatch intent")
        async with self._writer_lock:
            if self._active_by_session.get(operation.session_id) is not operation:
                return
            self.store.commit_success(
                intent_id=operation.intent_id,
                frame_id=operation.frame_id,
                session_id=operation.session_id,
                sequence=operation.sequence,
                channel_id=slot.channel_id,
                egress=operation.egress_address,
                snapshot_version=operation.snapshot_version,
                payload_len=len(operation.payload),
                result=result,
            )
            operation.state = "acked"
            self._last_acked[operation.session_id] = operation.sequence
            self._next_expected[operation.session_id] = max(
                self._next_expected.get(operation.session_id, operation.sequence + 1),
                operation.sequence + 1,
            )
            self._leases = self.store.load_leases()
            self._stats = self.store.load_stats()
            self._sessions = self.store.load_sessions()
            self._release_slot_locked(slot)
            self._active_by_session.pop(operation.session_id, None)
            self._operations.pop(operation.frame_id, None)
            outcome = Outcome(operation.signature, True, result=result)
            self._remember_outcome(operation.frame_id, outcome)
            operation.future.set_result(outcome)
            self._publish()
            self._signal_work_changed()
            self._scheduler_event.set()

    async def _finalize_failure(
        self,
        operation: Operation,
        slot: ChannelSlot,
        exc: GatewayError,
    ) -> None:
        if operation.intent_id is None:
            return
        async with self._writer_lock:
            if self._active_by_session.get(operation.session_id) is not operation:
                return
            queued_same_session = [
                item for item in self._queue if item.session_id == operation.session_id
            ]
            queued_abort_rows = [
                {"frame_id": item.frame_id, "sequence": item.sequence}
                for item in queued_same_session
            ]
            self.store.commit_failure(
                intent_id=operation.intent_id,
                frame_id=operation.frame_id,
                session_id=operation.session_id,
                sequence=operation.sequence,
                channel_id=slot.channel_id,
                egress=operation.egress_address,
                snapshot_version=operation.snapshot_version,
                error_code=exc.code,
                error_context=exc.context,
                abort_queued=queued_abort_rows,
            )
            operation.state = "failed"
            self._release_slot_locked(slot)
            self._active_by_session.pop(operation.session_id, None)
            self._operations.pop(operation.frame_id, None)
            self._next_expected[operation.session_id] = operation.sequence
            operation_outcome = Outcome(
                operation.signature,
                False,
                error_code=exc.code,
                error_message=exc.message,
                error_context=exc.context,
            )
            self._remember_outcome(operation.frame_id, operation_outcome)
            operation.future.set_result(operation_outcome)
            for queued in queued_same_session:
                self._queue.remove(queued)
                self._operations.pop(queued.frame_id, None)
                queued_outcome = Outcome(
                    queued.signature,
                    False,
                    error_code="E_FRAME_INVALID",
                    error_message="queued frame was aborted because a prior frame in the session failed",
                    error_context={
                        "reason": "prior_frame_in_session_failed",
                        "prior_frame_id": operation.frame_id,
                        "prior_sequence": operation.sequence,
                    },
                )
                self._remember_outcome(queued.frame_id, queued_outcome)
                queued.future.set_result(queued_outcome)
            self._leases = self.store.load_leases()
            self._stats = self.store.load_stats()
            self._publish()
            self._signal_work_changed()
            self._scheduler_event.set()

    # ------------------------------ helpers -------------------------------
    def _success_result(
        self,
        operation: Operation,
        slot: ChannelSlot,
        response: DecodedResponse,
        response_payload: Any,
        transport_meta: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "frame_id": operation.frame_id,
            "session_id": operation.session_id,
            "sequence": operation.sequence,
            "channel_id": slot.channel_id,
            "snapshot_version": operation.snapshot_version,
            "egress_label": operation.egress_label,
            "egress": operation.egress_address,
            "request_checksum": "sha256:"
            + hashlib.sha256(operation.frame_bytes[:-32]).hexdigest(),
            "response_checksum": response.checksum,
            "payload_bytes": len(operation.payload),
            "intent_id": operation.intent_id,
            "transport": transport_meta,
            "response": response_payload,
        }

    def _release_slot_locked(self, slot: ChannelSlot) -> None:
        slot.operation = None
        slot.acquired_at = None

    def _remember_outcome(self, frame_id: str, outcome: Outcome) -> None:
        self._completed[frame_id] = outcome
        self._completed.move_to_end(frame_id)
        while len(self._completed) > self._completed_cache_size:
            self._completed.popitem(last=False)

    def _outcome_from_persisted_frame(self, frame_id: str, signature: str) -> Outcome | None:
        row = self.store.get_frame(frame_id)
        if row is None:
            return None
        stored_signature = hashlib.sha256(
            (
                row["session_id"]
                + "\x00"
                + str(row["sequence"])
                + "\x00"
                + row["payload_hash"]
            ).encode("utf-8")
        ).hexdigest()
        result = row.get("result")
        if row["status"] == "acked" and result is not None:
            outcome = Outcome(stored_signature, True, result=result)
            self._remember_outcome(frame_id, outcome)
            return outcome
        if row["status"] in {"failed", "aborted", "retransmit_required"}:
            error_code = row.get("error_code") or "E_FRAME_INVALID"
            error_context = dict(row.get("error_context") or {})
            error_context.setdefault("persisted_frame_status", row["status"])
            outcome = Outcome(
                stored_signature,
                False,
                error_code=error_code,
                error_message="persisted frame outcome cannot be redispatched",
                error_context=error_context,
            )
            self._remember_outcome(frame_id, outcome)
            return outcome
        raise GatewayError(
            "E_RECOVERY_REQUIRED",
            "frame_id belongs to an incomplete persisted operation",
            {"frame_id": frame_id, "persisted_status": row["status"]},
        )

    def _raise_or_result(self, outcome: Outcome) -> dict[str, Any]:
        if outcome.ok:
            assert outcome.result is not None
            return outcome.result
        raise GatewayError(
            outcome.error_code or "E_INTERNAL",
            outcome.error_message or "frame failed",
            dict(outcome.error_context or {}),
        )

    def _materialize_payload(self, submission: FrameSubmission) -> bytes:
        if submission.payload_b64 is not None:
            try:
                return base64.b64decode(submission.payload_b64, validate=True)
            except ValueError as exc:
                raise GatewayError(
                    "E_FRAME_INVALID",
                    "payload_b64 is not valid base64",
                    {"field": "payload_b64"},
                ) from exc
        return json.dumps(
            submission.payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    def _block_sessions(self, sessions: tuple[str, ...]) -> None:
        if sessions:
            self._blocked_sessions.update(sessions)
            self._unblock_event.clear()
            self._publish()

    def _unblock_sessions(self, sessions: tuple[str, ...]) -> None:
        self._blocked_sessions.difference_update(sessions)
        if not self._blocked_sessions:
            self._unblock_event.set()
        self._publish()

    async def _wait_sessions_idle(self, sessions: tuple[str, ...]) -> None:
        relevant = set(sessions)
        while True:
            waiter = self._work_changed
            async with self._writer_lock:
                if not any(
                    item.session_id in relevant for item in self._queue
                ) and not any(
                    session_id in relevant for session_id in self._active_by_session
                ):
                    return
                waiter = self._work_changed
            await waiter.wait()

    def _signal_work_changed(self) -> None:
        self._work_changed.set()
        self._work_changed = asyncio.Event()

    # ------------------------------- reads --------------------------------
    def public_state(self) -> dict[str, Any]:
        return self._published

    def get_frame(self, frame_id: str) -> dict[str, Any]:
        row = self.store.get_frame(frame_id)
        if row is None:
            raise GatewayError(
                "E_NOT_FOUND", "frame was not found", {"frame_id": frame_id}
            )
        return row

    def list_frames(self, limit: int = 200, session_id: str | None = None) -> list[dict[str, Any]]:
        return self.store.list_frames(limit=limit, session_id=session_id)

    def list_traces(self, limit: int = 300, session_id: str | None = None) -> list[dict[str, Any]]:
        return self.store.list_traces(limit=limit, session_id=session_id)

    def list_intents(self, limit: int = 100, status: str | None = None) -> list[dict[str, Any]]:
        return self.store.list_intents(limit=limit, status=status)

    def latest_recovery(self) -> dict[str, Any] | None:
        return self.store.latest_recovery()

    def decode_observable_frame(self, frame_b64: str, max_payload: int = 60000) -> dict[str, Any]:
        try:
            raw = base64.b64decode(frame_b64, validate=True)
        except ValueError as exc:
            raise GatewayError(
                "E_FRAME_INVALID", "frame_b64 is not valid base64", {"field": "frame_b64"}
            ) from exc
        frame = decode_request(raw, max_payload=max_payload)
        return {
            "magic": "VGW1",
            "wire_version": 1,
            "session_id": frame.session_id,
            "channel": frame.channel,
            "sequence": frame.sequence,
            "payload_bytes": len(frame.payload),
            "checksum": frame.checksum,
            "payload_json": frame.payload_json(),
        }

    def _publish(self) -> None:
        snapshot_doc = None
        if self._snapshot is not None:
            snapshot_doc = model_dump_canonical(self._snapshot)
        queue_view = [
            {
                "position": index,
                "frame_id": item.frame_id,
                "session_id": item.session_id,
                "sequence": item.sequence,
                "priority": item.binding_priority,
                "channel_id": item.binding_channel,
                "egress": item.egress_address,
                "enqueued_at": item.enqueued_at,
            }
            for index, item in enumerate(self._queue)
        ]
        channel_view = [
            {
                "channel_id": slot.channel_id,
                "state": "busy" if slot.operation else "free",
                "session_id": slot.operation.session_id if slot.operation else None,
                "frame_id": slot.operation.frame_id if slot.operation else None,
                "sequence": slot.operation.sequence if slot.operation else None,
                "egress": slot.operation.egress_address if slot.operation else None,
                "acquired_at": slot.acquired_at,
            }
            for slot in self._channels
        ]
        active_view = [
            {
                "frame_id": operation.frame_id,
                "session_id": operation.session_id,
                "sequence": operation.sequence,
                "channel_id": operation.binding_channel,
                "egress": operation.egress_address,
                "priority": operation.binding_priority,
                "started_at": operation.started_at,
                "timeout_ms": operation.timeout_ms,
            }
            for operation in self._active_by_session.values()
        ]
        lease_view = sorted(self._leases.values(), key=lambda item: item["session_id"])
        session_view: dict[str, Any] = {}
        if self._snapshot is not None:
            for session_id, binding in sorted(self._snapshot.bindings.items()):
                active = self._active_by_session.get(session_id)
                session_view[session_id] = {
                    "session_id": session_id,
                    "priority": binding.priority,
                    "channel_id": binding.channel,
                    "egress_label": binding.egress,
                    "egress": self._snapshot.egress_endpoints[binding.egress],
                    "max_payload": binding.max_payload,
                    "last_acked_seq": self._last_acked.get(session_id, 0),
                    "next_expected_seq": self._next_expected.get(session_id, 1),
                    "queued_frames": sum(
                        1 for item in self._queue if item.session_id == session_id
                    ),
                    "active_frame_id": active.frame_id if active else None,
                    "blocked_for_rebuild": session_id in self._blocked_sessions,
                    "lease": self._leases.get(session_id),
                    "stats": self._stats.get(session_id),
                }
        self._published = {
            "snapshot": snapshot_doc,
            "version": self._snapshot.version if self._snapshot else None,
            "digest": self._snapshot.digest if self._snapshot else None,
            "channels": channel_view,
            "queue": queue_view,
            "queue_depth": len(queue_view),
            "queue_capacity": self._snapshot.queue_capacity if self._snapshot else None,
            "active": active_view,
            "leases": lease_view,
            "stats": dict(self._stats),
            "sessions": session_view,
            "blocked_sessions": sorted(self._blocked_sessions),
            "last_scheduler_round": self._last_round,
            "scheduler": {
                "round_budget_ms": self._snapshot.round_budget_ms if self._snapshot else None,
                "fixed_channels": self._snapshot.channel_count if self._snapshot else None,
                "running": self._running,
                "graceful_stop_requested": self._stopping,
            },
            "recovery": self._recovery_report,
            "generated_at": utc_now(),
            "started_at": self._started_at,
        }


def _split_address(address: str) -> tuple[str, int]:
    host, port_text = address.rsplit(":", 1)
    return host.strip("[]"), int(port_text)

