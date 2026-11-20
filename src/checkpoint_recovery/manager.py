from __future__ import annotations

import json
import os
import queue
import threading
import time
import uuid
from concurrent.futures import Future
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .config import ManagerConfig
from .db import Database
from .errors import ErrorCode, ServiceError
from .models import CheckpointState, ParsedCheckpointRequest, parse_checkpoint_request, utc_now
from .storage import CheckpointStorage


@dataclass(frozen=True, slots=True)
class WriteSubmission:
    request_id: str
    checkpoint_id: str
    round: int
    status: str
    reused: bool = False


@dataclass(slots=True)
class _WriteTask:
    request_id: str
    checkpoint_id: str
    parsed: ParsedCheckpointRequest
    future: Future


@dataclass(slots=True)
class _PruneTask:
    checkpoint_ids: tuple[str, ...] | None
    future: Future


@dataclass(slots=True)
class _WorkerState:
    stopping: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)


class CheckpointManager:
    """Single-process coordinator with one serial mutation/writer queue."""

    def __init__(self, config: ManagerConfig) -> None:
        self.config = config
        self.config.data_dir.mkdir(parents=True, exist_ok=True)
        self.db = Database(config.database_path)
        self.storage = CheckpointStorage(config)
        self._submit_lock = threading.RLock()
        self._mutation_lock = threading.RLock()
        self._futures: dict[str, Future] = {}
        self._queue: queue.PriorityQueue[tuple[int, _WriteTask | _PruneTask | None]] = queue.PriorityQueue()
        self._sequence = 0
        self._worker_state = _WorkerState()
        self.startup_recovery = self._recover_startup()
        self._writer = threading.Thread(target=self._writer_loop, name="checkpoint-single-writer", daemon=True)
        self._writer.start()

    def _next_sequence(self) -> int:
        with self._submit_lock:
            self._sequence += 1
            return self._sequence

    def _recover_startup(self) -> dict[str, Any]:
        """Turn durable half-states into facts before accepting new traffic."""

        started_at = utc_now()
        facts: dict[str, Any] = {
            "started_at": started_at,
            "writing_checkpoints_invalidated": [],
            "prune_intents_completed": [],
            "prune_intent_errors": [],
            "orphan_intent_files": [],
            "cleanup": {},
            "pending_write_requests_failed": [],
        }
        with self._mutation_lock:
            for intent in self.db.pending_intents():
                checkpoint_id = intent["checkpoint_id"]
                checkpoint = self.db.get_checkpoint(checkpoint_id)
                if checkpoint is None:
                    try:
                        self.db.finish_intent(intent["intent_id"], "ABORTED", "checkpoint_missing_during_recovery")
                    except Exception as exc:  # pragma: no cover - defensive durability path
                        facts["prune_intent_errors"].append({"intent_id": intent["intent_id"], "error": str(exc)})
                    continue
                try:
                    if checkpoint["status"] == CheckpointState.COMMITTED.value:
                        self.db.transition(
                            checkpoint_id,
                            CheckpointState.PRUNED,
                            "recovery_completed_pending_prune_intent",
                            {"intent_id": intent["intent_id"]},
                        )
                    current = self.db.get_checkpoint(checkpoint_id)
                    if current and current["status"] == CheckpointState.PRUNED.value:
                        self.storage.cleanup_checkpoint_artifacts(checkpoint_id)
                        self.db.clear_pruned_metadata(checkpoint_id)
                        self.db.finish_intent(intent["intent_id"], "DONE")
                        facts["prune_intents_completed"].append(
                            {"intent_id": intent["intent_id"], "checkpoint_id": checkpoint_id}
                        )
                    else:
                        self.db.finish_intent(
                            intent["intent_id"],
                            "ABORTED",
                            f"unexpected_state_during_recovery:{current['status'] if current else 'missing'}",
                        )
                except Exception as exc:
                    facts["prune_intent_errors"].append(
                        {"intent_id": intent["intent_id"], "checkpoint_id": checkpoint_id, "error": str(exc)}
                    )

            writing_ids = self.db.ids_by_state(CheckpointState.WRITING)
            for checkpoint_id in writing_ids:
                checkpoint = self.db.get_checkpoint(checkpoint_id)
                if checkpoint is None:
                    continue
                error = ServiceError(
                    ErrorCode.PROCESS_TERMINATED,
                    "writer process terminated before the checkpoint committed",
                    {"checkpoint_id": checkpoint_id},
                )
                try:
                    self.db.transition(
                        checkpoint_id,
                        CheckpointState.INVALID,
                        "forced_termination_recovered_incomplete_write",
                        {"temporary_artifacts_cleaned": True},
                    )
                    self.db.fail_write(
                        checkpoint_id,
                        checkpoint["request_id"],
                        error,
                        "forced_termination_recovered_incomplete_write",
                    )
                    removed = self.storage.cleanup_checkpoint_artifacts(checkpoint_id)
                    facts["writing_checkpoints_invalidated"].append(
                        {"checkpoint_id": checkpoint_id, "request_id": checkpoint["request_id"], "removed": removed}
                    )
                except Exception as exc:
                    facts["prune_intent_errors"].append(
                        {"checkpoint_id": checkpoint_id, "error": f"writing_recovery_failed:{exc}"}
                    )

            for request in self.db.pending_write_requests():
                checkpoint = self.db.get_checkpoint(request["checkpoint_id"])
                if checkpoint is None:
                    continue
                error = ServiceError(
                    ErrorCode.PROCESS_TERMINATED,
                    "write request was interrupted before a durable commit",
                    {"request_id": request["request_id"], "checkpoint_id": request["checkpoint_id"]},
                )
                self.db.fail_write(
                    request["checkpoint_id"], request["request_id"], error, "recovery_marked_pending_request_failed"
                )
                facts["pending_write_requests_failed"].append(request["request_id"])

            committed_ids = set(self.db.ids_by_state(CheckpointState.COMMITTED))
            facts["cleanup"] = self.storage.cleanup_orphans(committed_ids)
            known_intents = self.db.all_intent_ids()
            for intent_id, path in self.storage.intent_files().items():
                if intent_id in known_intents:
                    continue
                facts["orphan_intent_files"].append(intent_id)
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
            facts["finished_at"] = utc_now()
            orphan_intent_temps = []
            for path in self.storage.intent_temp_files():
                orphan_intent_temps.append(path.name)
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
            facts["orphan_intent_temp_files"] = orphan_intent_temps
            self.db.add_recovery_run(f"recovery-{uuid.uuid4().hex}", started_at, facts)
        return facts

    def _writer_loop(self) -> None:
        while True:
            _sequence, task = self._queue.get()
            try:
                if task is None:
                    return
                try:
                    if isinstance(task, _WriteTask):
                        result = self._execute_write(task)
                    else:
                        result = self._execute_prune(task)
                except BaseException as exc:  # normalize before handing to a waiting request
                    if isinstance(exc, ServiceError):
                        error = exc
                    else:
                        error = ServiceError(
                            ErrorCode.INTERNAL,
                            "single writer failed",
                            {"exception_type": type(exc).__name__, "message": str(exc)},
                        )
                    if not task.future.done():
                        task.future.set_exception(error)
                else:
                    if not task.future.done():
                        task.future.set_result(result)
            finally:
                self._queue.task_done()

    def _execute_write(self, task: _WriteTask) -> dict[str, Any]:
        checkpoint = self.db.get_checkpoint(task.checkpoint_id)
        if checkpoint is None:
            raise ServiceError(
                ErrorCode.CHECKPOINT_NOT_FOUND,
                "checkpoint record disappeared before writing",
                {"checkpoint_id": task.checkpoint_id},
            )
        if checkpoint["status"] != CheckpointState.WRITING.value:
            raise ServiceError(
                ErrorCode.INVALID_TRANSITION,
                "checkpoint is no longer in WRITING state",
                {"checkpoint_id": task.checkpoint_id, "state": checkpoint["status"]},
            )
        prepared = None
        committed = False
        try:
            def on_first_shard(_path: Path) -> None:
                if self.config.fault_crash_after_temp_files:
                    time.sleep(self.config.fault_crash_delay_seconds)
                    os._exit(91)

            prepared = self.storage.write_temp_checkpoint(
                task.checkpoint_id,
                task.parsed,
                on_first_shard=on_first_shard,
            )
            if self.config.fault_corrupt_after_temp_write:
                self.storage.corrupt_first_temp_shard(prepared)
            verification = self.storage.verify_temp_checkpoint(prepared)
            if not verification["valid"]:
                raise ServiceError(
                    ErrorCode.CHECKSUM_MISMATCH,
                    "temporary checkpoint digest verification failed; commit was not attempted",
                    {
                        "checkpoint_id": task.checkpoint_id,
                        "request_id": task.request_id,
                        "verification": verification,
                    },
                )
            self.storage.promote(prepared)
            relative_manifest = self.storage.relative_manifest_path(task.checkpoint_id)
            committed_result = {
                "checkpoint_id": task.checkpoint_id,
                "request_id": task.request_id,
                "round": task.parsed.round_number,
                "status": CheckpointState.COMMITTED.value,
            }
            with self._mutation_lock:
                self.db.complete_commit(
                    task.checkpoint_id,
                    relative_manifest,
                    prepared.manifest_sha256,
                    prepared.relative_shards,
                    committed_result,
                )
            committed = True
            final = self.db.get_checkpoint(task.checkpoint_id)
            if final is None:
                raise ServiceError(
                    ErrorCode.INTERNAL,
                    "checkpoint committed but could not be read back",
                    {"checkpoint_id": task.checkpoint_id},
                )
            return final
        except ServiceError as exc:
            current = self.db.get_checkpoint(task.checkpoint_id)
            durable_commit = committed or (current is not None and current["status"] == CheckpointState.COMMITTED.value)
            if prepared is not None and not durable_commit:
                self.storage.cleanup_checkpoint_artifacts(task.checkpoint_id)
            if not durable_commit:
                try:
                    self.db.fail_write(
                        task.checkpoint_id,
                        task.request_id,
                        exc,
                        "checkpoint_write_validation_failed",
                        exc.details,
                    )
                except Exception:
                    pass
            raise
        except OSError as exc:
            current = self.db.get_checkpoint(task.checkpoint_id)
            durable_commit = committed or (current is not None and current["status"] == CheckpointState.COMMITTED.value)
            if prepared is not None and not durable_commit:
                self.storage.cleanup_checkpoint_artifacts(task.checkpoint_id)
            error = ServiceError(
                ErrorCode.WRITE_FAILED,
                "checkpoint file write failed",
                {"checkpoint_id": task.checkpoint_id, "error": str(exc)},
            )
            if not durable_commit:
                try:
                    self.db.fail_write(task.checkpoint_id, task.request_id, error, "checkpoint_file_write_failed")
                except Exception:
                    pass
            raise error from exc
        except Exception as exc:
            current = self.db.get_checkpoint(task.checkpoint_id)
            durable_commit = committed or (current is not None and current["status"] == CheckpointState.COMMITTED.value)
            if prepared is not None and not durable_commit:
                self.storage.cleanup_checkpoint_artifacts(task.checkpoint_id)
            error = ServiceError(
                ErrorCode.WRITE_FAILED,
                "unexpected checkpoint write failure",
                {"checkpoint_id": task.checkpoint_id, "exception_type": type(exc).__name__, "message": str(exc)},
            )
            if not durable_commit:
                try:
                    self.db.fail_write(task.checkpoint_id, task.request_id, error, "checkpoint_write_failed")
                except Exception:
                    pass
            raise error from exc

    def submit_checkpoint(self, raw: Any, *, wait: bool = False) -> WriteSubmission:
        parsed = parse_checkpoint_request(raw)
        with self._submit_lock:
            existing_request = self.db.get_write_request(parsed.request_id)
            if existing_request is not None:
                existing_checkpoint = self.db.get_checkpoint(existing_request["checkpoint_id"])
                future = self._futures.get(parsed.request_id)
            else:
                existing_checkpoint = None
                future = None
                checkpoint_id = f"ckpt-{uuid.uuid4().hex}"
                future = Future()
                self.db.create_writing_checkpoint(checkpoint_id, parsed, self.config.max_writing)
                self._futures[parsed.request_id] = future
                self._queue.put(
                    (
                        self._next_sequence(),
                        _WriteTask(parsed.request_id, checkpoint_id, parsed, future),
                    )
                )
                submission = WriteSubmission(
                    request_id=parsed.request_id,
                    checkpoint_id=checkpoint_id,
                    round=parsed.round_number,
                    status=CheckpointState.WRITING.value,
                )
        if existing_request is None:
            if wait:
                result = self.wait_for_request(parsed.request_id)
                return WriteSubmission(
                    request_id=parsed.request_id,
                    checkpoint_id=result["checkpoint_id"],
                    round=result["round"],
                    status=result["status"],
                )
            return submission

        if existing_request["state"] == "ERROR":
            raise ServiceError.from_dict(json.loads(existing_request["error_json"]))
        # Idempotent duplicates wait for the original execution and reuse its
        # terminal result. Only the first submission is asynchronous by default.
        if future is not None:
            result = future.result()
            return WriteSubmission(
                request_id=parsed.request_id,
                checkpoint_id=result["checkpoint_id"],
                round=result["round"],
                status=result["status"],
                reused=True,
            )
        if existing_request["state"] == "SUCCESS" and existing_checkpoint is not None:
            return WriteSubmission(
                request_id=parsed.request_id,
                checkpoint_id=existing_checkpoint["checkpoint_id"],
                round=existing_checkpoint["round"],
                status=existing_checkpoint["status"],
                reused=True,
            )
        return WriteSubmission(
            request_id=parsed.request_id,
            checkpoint_id=existing_request["checkpoint_id"],
            round=existing_checkpoint["round"] if existing_checkpoint else parsed.round_number,
            status=CheckpointState.WRITING.value,
            reused=True,
        )

    def wait_for_request(self, request_id: str) -> dict[str, Any]:
        with self._submit_lock:
            future = self._futures.get(request_id)
        if future is not None:
            return future.result()
        request = self.db.get_write_request(request_id)
        if request is None:
            raise ServiceError(
                ErrorCode.CHECKPOINT_NOT_FOUND,
                "write request was not found",
                {"request_id": request_id},
            )
        if request["state"] == "ERROR":
            raise ServiceError.from_dict(json.loads(request["error_json"]))
        if request["state"] == "SUCCESS":
            checkpoint = self.db.get_checkpoint(request["checkpoint_id"])
            if checkpoint is not None:
                return checkpoint
        raise ServiceError(
            ErrorCode.PROCESS_TERMINATED,
            "write request has not reached a durable terminal result",
            {"request_id": request_id, "checkpoint_id": request["checkpoint_id"]},
        )

    def _execute_prune(self, task: _PruneTask) -> dict[str, Any]:
        with self._mutation_lock:
            committed = self.db.list_committed()
            if not committed:
                if task.checkpoint_ids:
                    raise ServiceError(
                        ErrorCode.CHECKPOINT_NOT_FOUND,
                        "no checkpoint matched the explicit prune request",
                        {"checkpoint_ids": list(task.checkpoint_ids)},
                    )
                return {
                    "pruned": [],
                    "skipped": [],
                    "retained": [],
                    "selection": {"mode": "auto", "candidates": []},
                }
            latest = committed[0]
            skipped: list[dict[str, Any]] = []
            selection: dict[str, Any]
            if task.checkpoint_ids is not None:
                candidates: list[dict[str, Any]] = []
                for checkpoint_id in task.checkpoint_ids:
                    checkpoint = self.db.get_checkpoint(checkpoint_id)
                    if checkpoint is None:
                        raise ServiceError(
                            ErrorCode.CHECKPOINT_NOT_FOUND,
                            "checkpoint was not found",
                            {"checkpoint_id": checkpoint_id},
                        )
                    if checkpoint["status"] != CheckpointState.COMMITTED.value:
                        raise ServiceError(
                            ErrorCode.PRUNE_CONFLICT,
                            "only COMMITTED checkpoints can be pruned",
                            {"checkpoint_id": checkpoint_id, "status": checkpoint["status"]},
                        )
                    if checkpoint_id == latest["checkpoint_id"]:
                        raise ServiceError(
                            ErrorCode.PRUNE_CONFLICT,
                            "the latest COMMITTED checkpoint is the recovery baseline and cannot be pruned",
                            {
                                "checkpoint_id": checkpoint_id,
                                "skip_reason": "latest_committed_checkpoint_is_recovery_protected",
                                "latest_checkpoint_id": latest["checkpoint_id"],
                            },
                        )
                    candidates.append(checkpoint)
                selection = {"mode": "explicit", "candidates": [item["checkpoint_id"] for item in candidates]}
            else:
                candidates, selection = self._select_auto_prune_candidates(committed)
            pruned: list[dict[str, Any]] = []
            for candidate in candidates:
                checkpoint_id = candidate["checkpoint_id"]
                fresh = self.db.get_checkpoint(checkpoint_id)
                if fresh is None:
                    skipped.append({"checkpoint_id": checkpoint_id, "reason": "checkpoint_missing"})
                    continue
                if fresh["status"] != CheckpointState.COMMITTED.value:
                    skipped.append(
                        {"checkpoint_id": checkpoint_id, "reason": f"state_changed:{fresh['status']}"}
                    )
                    continue
                current_committed = self.db.list_committed()
                if not current_committed:
                    skipped.append({"checkpoint_id": checkpoint_id, "reason": "no_committed_baseline"})
                    continue
                if checkpoint_id == current_committed[0]["checkpoint_id"]:
                    skipped.append(
                        {
                            "checkpoint_id": checkpoint_id,
                            "reason": "latest_committed_checkpoint_is_recovery_protected",
                        }
                    )
                    continue
                intent_id = f"prune-{uuid.uuid4().hex}"
                intent_payload = {
                    "intent_id": intent_id,
                    "operation": "PRUNE",
                    "checkpoint_id": checkpoint_id,
                    "round": fresh["round"],
                    "created_at": utc_now(),
                    "reason": "retention_policy",
                }
                self.storage.write_intent_file(intent_id, intent_payload)
                self.db.create_intent(intent_id, checkpoint_id, intent_payload)
                try:
                    self.db.transition(
                        checkpoint_id,
                        CheckpointState.PRUNED,
                        "retention_policy_prune_intent_durably_recorded",
                        {"intent_id": intent_id},
                    )
                except ServiceError:
                    self.db.finish_intent(intent_id, "ABORTED", "state_changed_before_prune")
                    raise
                try:
                    removed = self.storage.cleanup_checkpoint_artifacts(checkpoint_id)
                    self.db.clear_pruned_metadata(checkpoint_id)
                    self.db.finish_intent(intent_id, "DONE")
                except OSError as exc:
                    self.db.finish_intent(intent_id, "PENDING", str(exc))
                    skipped.append(
                        {
                            "checkpoint_id": checkpoint_id,
                            "reason": "physical_cleanup_incomplete",
                            "error": str(exc),
                            "intent_id": intent_id,
                        }
                    )
                    continue
                pruned.append(
                    {
                        "checkpoint_id": checkpoint_id,
                        "round": fresh["round"],
                        "intent_id": intent_id,
                        "removed_paths": removed,
                    }
                )
            retained = self.db.list_committed()
            response = {
                "pruned": pruned,
                "skipped": skipped,
                "retained": [
                    {
                        "checkpoint_id": item["checkpoint_id"],
                        "round": item["round"],
                        "committed_at": item["committed_at"],
                    }
                    for item in retained
                ],
                "selection": selection,
            }
            if any(item["reason"] == "physical_cleanup_incomplete" for item in skipped):
                raise ServiceError(
                    ErrorCode.PRUNE_IO_ERROR,
                    "logical prune committed but physical cleanup is incomplete",
                    response,
                )
            return response

    def _select_auto_prune_candidates(
        self, committed: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        newest = committed[0]
        candidates: list[dict[str, Any]] = []
        protected: list[dict[str, Any]] = [
            {"checkpoint_id": newest["checkpoint_id"], "round": newest["round"], "reason": "latest_baseline"}
        ]
        retained_rounds = [newest["round"]]
        for checkpoint in committed[1:]:
            if len(retained_rounds) >= self.config.max_checkpoints:
                candidates.append(checkpoint)
                continue
            spacing = retained_rounds[-1] - checkpoint["round"]
            if spacing >= self.config.min_retain_round_interval:
                retained_rounds.append(checkpoint["round"])
                protected.append(
                    {
                        "checkpoint_id": checkpoint["checkpoint_id"],
                        "round": checkpoint["round"],
                        "reason": "retained_by_max_count_and_round_interval",
                    }
                )
            else:
                candidates.append(checkpoint)
        return candidates, {
            "mode": "auto",
            "latest_checkpoint_id": newest["checkpoint_id"],
            "latest_round": newest["round"],
            "max_checkpoints": self.config.max_checkpoints,
            "min_retain_round_interval": self.config.min_retain_round_interval,
            "protected": protected,
            "retained_rounds": retained_rounds,
            "candidates": [item["checkpoint_id"] for item in candidates],
        }

    def prune(self, checkpoint_ids: list[str] | None = None, *, wait: bool = True) -> dict[str, Any]:
        future: Future = Future()
        task = _PruneTask(tuple(checkpoint_ids) if checkpoint_ids is not None else None, future)
        self._queue.put((self._next_sequence(), task))
        if wait:
            return future.result()
        return {"status": "QUEUED"}

    def transition_checkpoint(self, checkpoint_id: str, target_state: str, reason: str) -> dict[str, Any]:
        try:
            target = CheckpointState(target_state.upper())
        except ValueError as exc:
            raise ServiceError(
                ErrorCode.INVALID_TRANSITION,
                "unknown target state",
                {"target_state": target_state},
            ) from exc
        if target not in {CheckpointState.INVALID}:
            raise ServiceError(
                ErrorCode.INVALID_TRANSITION,
                "COMMITTED and PRUNED states may only be produced by the commit/prune protocol",
                {"target_state": target.value},
            )
        with self._mutation_lock:
            self.db.transition(checkpoint_id, target, reason or "manual_invalidation")
            checkpoint = self.db.get_checkpoint(checkpoint_id)
            assert checkpoint is not None
            return checkpoint

    def list_checkpoints(self, status: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        if status is not None:
            try:
                status = CheckpointState(status.upper()).value
            except ValueError as exc:
                raise ServiceError(
                    ErrorCode.INVALID_REQUEST,
                    "unknown checkpoint status filter",
                    {"status": status},
                ) from exc
        return self.db.list_checkpoints(status, limit)

    def get_checkpoint(self, checkpoint_id: str) -> dict[str, Any]:
        checkpoint = self.db.get_checkpoint(checkpoint_id)
        if checkpoint is None:
            raise ServiceError(
                ErrorCode.CHECKPOINT_NOT_FOUND,
                "checkpoint was not found",
                {"checkpoint_id": checkpoint_id},
            )
        return checkpoint

    def get_transitions(self, checkpoint_id: str) -> list[dict[str, Any]]:
        if self.db.get_checkpoint_row(checkpoint_id) is None:
            raise ServiceError(
                ErrorCode.CHECKPOINT_NOT_FOUND,
                "checkpoint was not found",
                {"checkpoint_id": checkpoint_id},
            )
        return self.db.transitions_for(checkpoint_id)

    def get_write_request(self, request_id: str) -> dict[str, Any]:
        request = self.db.get_write_request(request_id)
        if request is None:
            raise ServiceError(
                ErrorCode.CHECKPOINT_NOT_FOUND,
                "write request was not found",
                {"request_id": request_id},
            )
        result = dict(request)
        if result.get("result_json"):
            result["result"] = json.loads(result.pop("result_json"))
        else:
            result.pop("result_json", None)
        if result.get("error_json"):
            result["error"] = json.loads(result.pop("error_json"))
        else:
            result.pop("error_json", None)
        return result

    def get_recovery_baseline(self) -> dict[str, Any]:
        with self._mutation_lock:
            committed = self.db.list_committed()
            rejected: list[dict[str, Any]] = []
            for checkpoint in committed:
                verification = self.storage.verify_committed_checkpoint(checkpoint)
                if verification["valid"]:
                    self.db.mark_shards_verified(checkpoint["checkpoint_id"])
                    latest = self.db.get_checkpoint(checkpoint["checkpoint_id"])
                    assert latest is not None
                    return {
                        "found": True,
                        "checkpoint": latest,
                        "verification": verification,
                        "fallback": {
                            "occurred": bool(rejected),
                            "rejected_checkpoints": rejected,
                            "reason": "latest_committed_checkpoint_failed_verification" if rejected else None,
                        },
                    }
                rejection_reason = "shard_digest_verification_failed"
                rejected.append(
                    {
                        "checkpoint_id": checkpoint["checkpoint_id"],
                        "round": checkpoint["round"],
                        "reason": rejection_reason,
                        "verification": verification,
                    }
                )
                self.db.transition(
                    checkpoint["checkpoint_id"],
                    CheckpointState.INVALID,
                    rejection_reason,
                    {"verification": verification, "source": "recovery_baseline_scan"},
                )
            raise ServiceError(
                ErrorCode.CHECKPOINT_NOT_FOUND,
                "no intact COMMITTED checkpoint is available for recovery",
                {"rejected_checkpoints": rejected},
            )

    def get_recovery_report(
        self,
        target_round: int | None = None,
        target_scheduler_position: int | None = None,
        target_data_cursor: Any | None = None,
    ) -> dict[str, Any]:
        baseline = self.get_recovery_baseline()
        checkpoint = baseline["checkpoint"]
        verification = baseline["verification"]
        if target_round is None:
            target_round = checkpoint["round"]
        if not isinstance(target_round, int) or isinstance(target_round, bool) or target_round < 0:
            raise ServiceError(ErrorCode.INVALID_REQUEST, "target_round must be an integer >= 0")
        rounds_to_replay = max(0, target_round - checkpoint["round"])
        optimizer_available = (
            verification["optimizer"]["total"] > 0
            and verification["optimizer"]["verified"] == verification["optimizer"]["total"]
        )
        scheduler_steps = None
        if target_scheduler_position is not None:
            scheduler_steps = target_scheduler_position - checkpoint["scheduler_position"]
        cursor_changed = target_data_cursor is not None and target_data_cursor != checkpoint["data_cursor"]
        latest_recovery = self.db.latest_recovery_run()
        return {
            "report_version": 1,
            "generated_at": utc_now(),
            "persisted_facts_only": True,
            "exit_report_available": False,
            "caveat": (
                "强制终止后仅依据已持久化的 SQLite 记录、清单、分片与意图日志生成恢复事实；"
                "不承诺恢复训练进程未持久化的退出状态或内存中的目标进度。"
            ),
            "recovery_baseline": {
                "checkpoint_id": checkpoint["checkpoint_id"],
                "round": checkpoint["round"],
                "scheduler_position": checkpoint["scheduler_position"],
                "data_cursor": checkpoint["data_cursor"],
                "committed_at": checkpoint["committed_at"],
            },
            "target": {
                "round": target_round,
                "scheduler_position": target_scheduler_position,
                "data_cursor": target_data_cursor,
            },
            "difference_report": {
                "rounds_to_replay": rounds_to_replay,
                "scheduler_steps_to_advance": scheduler_steps,
                "data_cursor_rewind_required": cursor_changed,
                "optimizer_momentum_available": optimizer_available,
                "params_shards_verified": verification["params"],
                "optimizer_shards_verified": verification["optimizer"],
                "shard_digest_conclusion": "PASS" if verification["valid"] else "FAIL",
                "fallback_occurred": baseline["fallback"]["occurred"],
                "fallback_reason": baseline["fallback"]["reason"],
                "rejected_checkpoints": baseline["fallback"]["rejected_checkpoints"],
            },
            "latest_startup_recovery": latest_recovery,
            "baseline_transitions": self.db.transitions_for(checkpoint["checkpoint_id"]),
        }

    def shutdown(self, timeout: float = 30.0) -> None:
        with self._worker_state.lock:
            if self._worker_state.stopping:
                return
            self._worker_state.stopping = True
        self._queue.put((10**18, None))
        self._writer.join(timeout=timeout)

    def __enter__(self) -> "CheckpointManager":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.shutdown()

