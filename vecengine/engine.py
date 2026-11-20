from __future__ import annotations

import hashlib
import json
import math
import os
import re
import threading
import time
import uuid
from pathlib import Path
from typing import Any

import numpy as np

from .errors import (
    BusinessError,
    DuplicateRequestError,
    ExecutionError,
    PlanError,
    ResultNotFoundError,
    SessionAbortedError,
    SessionNotFoundError,
    SessionNotReadyError,
)
from .evaluator import ResultTable, execute_plan
from .model import BOOL, FLOAT64, INT64, DatasetRegistry
from .persistence import Store, utc_now
from .plan import (
    canonical_plan_for_hash,
    parse_plan,
    plan_summary,
)
from .selection import EngineConfig, choose_execution, normalize_execution


TERMINAL_STATUSES = {"ready", "failed", "aborted"}
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")


class ExecutionEngine:
    def __init__(
        self,
        *,
        db_path: str | Path,
        data_dir: str | Path,
        results_dir: str | Path,
        config: EngineConfig | None = None,
        duplicate_wait_seconds: float = 600.0,
    ) -> None:
        self.config = config or EngineConfig()
        self.registry = DatasetRegistry(data_dir)
        self.results_dir = Path(results_dir).resolve()
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.store = Store(db_path)
        self.duplicate_wait_seconds = duplicate_wait_seconds
        self._submission_lock = threading.Lock()
        self._writer_lock = threading.Lock()
        self._terminal_condition = threading.Condition()
        self.startup_recovery_reports = self.store.recover_incomplete_sessions()
        self._cleanup_orphan_result_files()

    def _cleanup_orphan_result_files(self) -> None:
        referenced = {
            str(Path(path).resolve()) for path in self.store.ready_result_paths() if path
        }
        for path in self.results_dir.glob("*.npz"):
            resolved = str(path.resolve())
            if resolved not in referenced:
                try:
                    path.unlink()
                except OSError:
                    pass
        for path in self.results_dir.glob("*.tmp.npz"):
            try:
                path.unlink()
            except OSError:
                pass

    def submit(self, request_id: Any, raw_plan: Any, raw_execution: Any) -> dict[str, Any]:
        if not isinstance(request_id, str) or not _REQUEST_ID_RE.fullmatch(request_id):
            raise PlanError("request_id must match [A-Za-z0-9._:-]{1,128}")
        options = normalize_execution(raw_execution, self.config)
        plan = parse_plan(raw_plan, self.registry.get_spec)
        selection = choose_execution(plan, options, self.config)
        normalized_execution = options.to_dict()
        payload_hash = canonical_plan_for_hash(raw_plan, normalized_execution)
        summary = plan_summary(plan)

        new_session_id: str | None = None
        reused = False
        with self._submission_lock:
            existing = self.store.find_by_request_id(request_id)
            if existing is not None:
                if existing["payload_hash"] != payload_hash:
                    raise DuplicateRequestError(
                        "request_id was already used with a different plan or execution options",
                        details={"request_id": request_id, "session_id": existing["session_id"]},
                    )
                reused = True
            else:
                new_session_id = uuid.uuid4().hex
                submit_intent = self.store.create_intent(
                    session_id=new_session_id,
                    request_id=request_id,
                    kind="submit_session",
                    detail={"payload_hash": payload_hash},
                )
                try:
                    self.store.create_session(
                        session_id=new_session_id,
                        request_id=request_id,
                        payload_hash=payload_hash,
                        plan=raw_plan,
                        plan_summary=summary,
                        execution=normalized_execution,
                        dataset=plan.dataset,
                        total_rows=plan.spec.row_count,
                        submit_intent_id=submit_intent,
                    )
                except Exception:
                    # A unique-request race is impossible under _submission_lock, but a
                    # stale intent is still recoverable rather than silently discarded.
                    raise

        if reused:
            session = self._wait_for_terminal(str(existing["session_id"]))
            reuse_intent = self.store.create_intent(
                session_id=str(session["session_id"]),
                request_id=request_id,
                kind="duplicate_reuse",
                detail={"payload_hash": payload_hash},
            )
            self.store.increment_reuse_count(str(session["session_id"]), reuse_intent)
            session = self.store.get_session(str(session["session_id"])) or session
            self._raise_if_session_unsuccessful(session)
            return self._session_payload(session, reused=True)

        assert new_session_id is not None
        try:
            with self._writer_lock:
                self._execute_session(new_session_id, plan, options, selection)
        except MemoryError as exc:
            message = "actual allocation failed while executing the plan"
            self._record_failure(new_session_id, "E_MEMORY_BUDGET", message)
            self._notify_terminal()
            raise MemoryBudgetError(
                message,
                details={
                    "batch_size": selection.batch_size,
                    "estimated_peak_bytes": selection.estimated_peak_bytes,
                    "budget_bytes": options.memory_budget_bytes,
                },
            ) from exc
        except BusinessError as exc:
            self._record_failure(new_session_id, exc.code, exc.message)
            self._notify_terminal()
            raise
        except Exception as exc:
            message = f"unclassified execution failure: {type(exc).__name__}: {exc}"
            self._record_failure(new_session_id, "E_EXECUTION_FAILED", message)
            self._notify_terminal()
            raise ExecutionError(message, details={"exception_type": type(exc).__name__}) from exc
        self._notify_terminal()
        session = self.store.get_session(new_session_id)
        if session is None:
            raise SessionNotFoundError("session disappeared after execution")
        self._raise_if_session_unsuccessful(session)
        return self._session_payload(session, reused=False)

    def _record_failure(self, session_id: str, error_code: str, error_message: str) -> None:
        failure_intent = self.store.create_intent(
            session_id=session_id,
            request_id=None,
            kind="fail_session",
            detail={"error_code": error_code, "error_message": error_message},
        )
        self.store.mark_failed(
            session_id=session_id,
            error_code=error_code,
            error_message=error_message,
            intent_id=failure_intent,
        )

    def _execute_session(
        self,
        session_id: str,
        plan: Any,
        options: Any,
        selection: Any,
    ) -> None:
        run_intent = self.store.create_intent(
            session_id=session_id,
            request_id=None,
            kind="execute_session",
            detail={"path": selection.path, "batch_size": selection.batch_size},
        )
        self.store.mark_running(session_id, run_intent, selection.to_dict())
        dataset = self.registry.load(plan.dataset)
        current_intent: dict[str, Any] = {"id": 0, "started": 0.0, "stat": None}

        def before_batch(batch_index: int, row_start: int, row_end: int) -> None:
            current_intent["id"] = self.store.create_intent(
                session_id=session_id,
                request_id=None,
                kind="execute_batch",
                batch_index=batch_index,
                detail={
                    "row_start": row_start,
                    "row_end": row_end,
                    "path": selection.path,
                    "batch_size": selection.batch_size,
                },
            )
            current_intent["started"] = time.perf_counter()

        def after_batch(stat: dict[str, Any]) -> None:
            stat = dict(stat)
            stat["elapsed_ms"] = round(
                (time.perf_counter() - float(current_intent["started"])) * 1000.0, 3
            )
            current_intent["stat"] = stat
            self.store.record_batch_and_checkpoint(
                session_id=session_id,
                stat=stat,
                processed_rows=int(stat["row_end"]),
                intent_id=int(current_intent["id"]),
            )

        result = execute_plan(
            plan,
            dataset,
            options,
            selection,
            before_batch=before_batch,
            after_batch=after_batch,
        )
        expected_batches = (
            0 if plan.spec.row_count == 0 else math.ceil(plan.spec.row_count / selection.batch_size)
        )
        session = self.store.get_session(session_id)
        if session is None:
            raise SessionNotFoundError("session disappeared during execution")
        if int(session["completed_batches"]) != expected_batches:
            raise ExecutionError(
                "batch checkpoint count does not match the plan",
                details={
                    "expected_batches": expected_batches,
                    "completed_batches": int(session["completed_batches"]),
                },
            )

        finalize_intent = self.store.create_intent(
            session_id=session_id,
            request_id=None,
            kind="finalize_and_publish",
            detail={"output_rows": result.row_count, "output_columns": len(result.names)},
        )
        final_path = self.results_dir / f"{session_id}.npz"
        temp_path = self.results_dir / f"{session_id}.tmp.npz"
        try:
            self._write_result_archive(temp_path, final_path, result, plan, options, selection)
            result_hash = _sha256_file(final_path)
            self.store.publish_result(
                session_id=session_id,
                intent_id=finalize_intent,
                result_path=str(final_path),
                result_sha256=result_hash,
                output_rows=result.row_count,
                output_columns=len(result.names),
            )
        except Exception:
            try:
                temp_path.unlink()
            except OSError:
                pass
            raise

    def _write_result_archive(
        self,
        temp_path: Path,
        final_path: Path,
        result: ResultTable,
        plan: Any,
        options: Any,
        selection: Any,
    ) -> None:
        metadata = {
            "format_version": 1,
            "null_bitmap_convention": "true means value is valid",
            "path": selection.path,
            "batch_size": selection.batch_size,
            "columns": [
                {"name": name, "dtype": dtype}
                for name, dtype in zip(result.names, result.dtypes)
            ],
            "row_count": result.row_count,
            "float_format": {
                "decimal_places": options.float_decimal_places,
                "rounding": "round-half-even",
                "json_mode": "fixed_decimal_strings",
                "note": "NPZ stores full binary values; JSON display is rounded only for output.",
            },
            "integer_overflow": options.integer_overflow,
            "float_compare": options.float_compare,
            "compensated_sum": "Neumaier",
            "selection": selection.to_dict(),
        }
        arrays: dict[str, np.ndarray] = {}
        for name, dtype in zip(result.names, result.dtypes):
            arrays[f"{name}__values"] = result.values[name]
            arrays[f"{name}__valid"] = result.valid[name]
        arrays["__meta__"] = np.asarray(json.dumps(metadata, ensure_ascii=False, sort_keys=True))
        np.savez(temp_path, **arrays)
        os.replace(temp_path, final_path)

    def _wait_for_terminal(self, session_id: str) -> dict[str, Any]:
        deadline = time.monotonic() + self.duplicate_wait_seconds
        while True:
            with self._terminal_condition:
                session = self.store.get_session(session_id)
                if session is None:
                    raise SessionNotFoundError(
                        "session not found", details={"session_id": session_id}
                    )
                if session["status"] in TERMINAL_STATUSES:
                    return session
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise DuplicateRequestError(
                        "another submission with the same request_id is still executing",
                        details={"session_id": session_id, "wait_seconds": self.duplicate_wait_seconds},
                    )
                self._terminal_condition.wait(min(remaining, 1.0))

    def _notify_terminal(self) -> None:
        with self._terminal_condition:
            self._terminal_condition.notify_all()

    @staticmethod
    def _raise_if_session_unsuccessful(session: dict[str, Any]) -> None:
        if session["status"] == "ready":
            return
        code = session.get("error_code") or "E_EXECUTION_FAILED"
        message = session.get("error_message") or f"session ended with status {session['status']}"
        details = {"session_id": session["session_id"], "status": session["status"]}
        if code == "E_SESSION_ABORTED":
            raise SessionAbortedError(message, details=details)
        if code == "E_MEMORY_BUDGET":
            raise MemoryBudgetError(message, details=details)
        if code == "E_PLAN_INVALID":
            raise PlanError(message, details=details)
        raise ExecutionError(message, details=details | {"stored_error_code": code})

    def get_session(self, session_id: str) -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if session is None:
            raise SessionNotFoundError("session not found", details={"session_id": session_id})
        return self._session_payload(session, reused=None)

    def _session_payload(self, session: dict[str, Any], reused: bool | None) -> dict[str, Any]:
        selection = _decode_json(session.get("selection_json"), None)
        summary = _decode_json(session.get("plan_summary_json"), {})
        error = None
        if session.get("error_code"):
            error = {"code": session["error_code"], "message": session.get("error_message")}
        payload = {
            "session_id": session["session_id"],
            "request_id": session["request_id"],
            "status": session["status"],
            "dataset": session["dataset"],
            "total_rows": session["total_rows"],
            "output_rows": session.get("output_rows"),
            "output_columns": session.get("output_columns"),
            "path": session.get("path"),
            "batch_size": session.get("batch_size"),
            "selection": selection,
            "plan_summary": summary,
            "completed_batches": session["completed_batches"],
            "processed_rows": session["processed_rows"],
            "reuse_count": session.get("reuse_count", 0),
            "created_at": session["created_at"],
            "started_at": session.get("started_at"),
            "finished_at": session.get("finished_at"),
            "result_sha256": session.get("result_sha256"),
            "result_url": (
                f"/sessions/{session['session_id']}/result"
                if session["status"] == "ready"
                else None
            ),
            "result_download_url": (
                f"/sessions/{session['session_id']}/result?format=npz"
                if session["status"] == "ready"
                else None
            ),
            "batch_stats_url": f"/sessions/{session['session_id']}/batches",
            "error": error,
        }
        if reused is not None:
            payload["reused"] = reused
        return payload

    def get_batches(self, session_id: str) -> dict[str, Any]:
        if self.store.get_session(session_id) is None:
            raise SessionNotFoundError("session not found", details={"session_id": session_id})
        return {"session_id": session_id, "batches": self.store.list_batch_stats(session_id)}

    def get_result_page(self, session_id: str, offset: int, limit: int) -> dict[str, Any]:
        session = self._ready_session(session_id)
        if offset < 0 or limit <= 0 or limit > 5000:
            raise PlanError("result pagination requires offset>=0 and 1<=limit<=5000")
        path = self._result_path(session)
        execution = _decode_json(session.get("execution_json"), {})
        decimal_places = int(execution.get("float_decimal_places", 6))
        with np.load(path, allow_pickle=False) as archive:
            metadata = json.loads(str(archive["__meta__"].item()))
            row_count = int(metadata["row_count"])
            start = min(offset, row_count)
            stop = min(row_count, start + limit)
            columns = []
            for column in metadata["columns"]:
                name = column["name"]
                dtype = column["dtype"]
                vals = archive[f"{name}__values"][start:stop]
                valid = archive[f"{name}__valid"][start:stop]
                columns.append(
                    {
                        "name": name,
                        "dtype": dtype,
                        "values": [
                            _json_scalar(value, dtype, decimal_places, bool(is_valid))
                            for value, is_valid in zip(vals, valid)
                        ],
                        "valid": [bool(value) for value in valid],
                    }
                )
        return {
            "session_id": session_id,
            "row_count": row_count,
            "offset": start,
            "limit": stop - start,
            "columns": columns,
            "float_format": {
                "decimal_places": decimal_places,
                "rounding": "round-half-even",
                "json_mode": "fixed_decimal_strings",
                "note": "Stored values are full precision; this field is rounded for JSON display only.",
            },
            "path": session.get("path"),
            "batch_size": session.get("batch_size"),
        }

    def get_result_file(self, session_id: str) -> tuple[Path, str]:
        session = self._ready_session(session_id)
        return self._result_path(session), str(session.get("result_sha256"))

    def _ready_session(self, session_id: str) -> dict[str, Any]:
        session = self.store.get_session(session_id)
        if session is None:
            raise SessionNotFoundError("session not found", details={"session_id": session_id})
        if session["status"] in ("queued", "running"):
            raise SessionNotReadyError(
                "session result is not ready",
                details={"session_id": session_id, "status": session["status"]},
            )
        if session["status"] == "aborted":
            raise SessionAbortedError(
                "session was aborted; no complete result exists",
                details={"session_id": session_id},
            )
        if session["status"] == "failed":
            raise ExecutionError(
                session.get("error_message") or "session execution failed",
                details={"session_id": session_id, "stored_error_code": session.get("error_code")},
            )
        return session

    def _result_path(self, session: dict[str, Any]) -> Path:
        raw = session.get("result_path")
        if not raw:
            raise ResultNotFoundError("session has no persisted result")
        path = Path(raw).resolve()
        if self.results_dir != path.parent and self.results_dir not in path.parents:
            raise ResultNotFoundError("persisted result path is outside the result directory")
        if not path.exists():
            raise ResultNotFoundError("persisted result file is missing", details={"path": str(path)})
        return path

    def recovery_reports(self) -> dict[str, Any]:
        return {"reports": self.store.list_recovery_reports()}

    def recovery_report(self, session_id: str) -> dict[str, Any]:
        if self.store.get_session(session_id) is None:
            # Orphan-intent reports intentionally have no session row.
            report = self.store.get_recovery_report(session_id)
            if report is None:
                raise SessionNotFoundError("session not found", details={"session_id": session_id})
            return report
        report = self.store.get_recovery_report(session_id)
        if report is None:
            raise SessionNotFoundError(
                "no recovery report exists for this session",
                details={"session_id": session_id},
            )
        return report

    def intents(self, session_id: str) -> dict[str, Any]:
        if self.store.get_session(session_id) is None:
            raise SessionNotFoundError("session not found", details={"session_id": session_id})
        return {"session_id": session_id, "intents": self.store.list_intents(session_id)}


def _decode_json(raw: Any, default: Any) -> Any:
    if raw is None:
        return default
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return default


def _json_scalar(value: Any, dtype: str, decimal_places: int, valid: bool) -> Any:
    if not valid:
        return None
    if dtype == INT64:
        return int(value)
    if dtype == BOOL:
        return bool(value)
    number = float(value)
    if math.isnan(number):
        return "NaN"
    if math.isinf(number):
        return "Infinity" if number > 0 else "-Infinity"
    return format(number, f".{decimal_places}f")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
