from __future__ import annotations

import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Callable, Iterator

from .config import WorkbenchConfig
from .database import Database
from .errors import (
    BusinessError,
    invalid_sample,
    invalid_snapshot,
    invalid_transition,
)
from .planner import DiagnosisPlanner
from .repository import Repository
from .state_machine import SessionStatus
from .util import Clock, canonical_json
from .validation import normalize_sample_batch, normalize_snapshot
from .verification import (
    VerificationExecutionError,
    verify_rewrite,
)


class CrashController:
    """Deterministic crash injection used by the recovery demo and tests only."""

    def __init__(self, point: str | None = None) -> None:
        self.point = point or os.environ.get("SLOWQUERY_CRASH_POINT")

    def hit(self, point: str) -> None:
        if self.point and self.point == point:
            os._exit(86)


class SlowQueryService:
    def __init__(
        self,
        config: WorkbenchConfig,
        *,
        recover_on_init: bool = True,
        crash_point: str | None = None,
        clock: Clock | None = None,
        verifier: Callable[..., dict[str, Any]] | None = None,
    ) -> None:
        self.config = config
        self.clock = clock or Clock()
        self.db = Database(config.database_path, config.sqlite_busy_timeout_ms)
        self.db.initialize()
        self.repo = Repository(self.db, self.clock)
        self.planner = DiagnosisPlanner(config)
        self.crash = CrashController(crash_point)
        self.writer_lock = threading.RLock()
        self._session_locks_guard = threading.Lock()
        self._session_locks: dict[str, threading.RLock] = {}
        self._verifier = verifier or verify_rewrite
        self.last_recovery: dict[str, Any] | None = None
        if recover_on_init:
            self.last_recovery = self.recover_incomplete()

    @contextmanager
    def _session_guard(self, session_id: str) -> Iterator[None]:
        with self._session_locks_guard:
            lock = self._session_locks.setdefault(session_id, threading.RLock())
        lock.acquire()
        try:
            yield
        finally:
            lock.release()

    def _session_view(self, session_id: str) -> dict[str, Any]:
        return self.repo.get_session_and_suggestions(session_id)

    # ------------------------------------------------------------------ data setup
    def submit_samples(self, raw: Any) -> dict[str, Any]:
        samples = normalize_sample_batch(raw)
        batch_id = "batch_" + uuid.uuid4().hex
        with self.writer_lock:
            intent_id = self.repo.start_intent(
                kind="SAMPLE_BATCH",
                session_id=None,
                request={"batch_id": batch_id, "sample_count": len(samples)},
            )
            self.crash.hit("SAMPLE_AFTER_INTENT")
            try:
                with self.db.write_transaction() as conn:
                    stats = self.repo.insert_samples(conn, samples, batch_id=batch_id)
                    self.repo.update_intent(
                        conn,
                        intent_id,
                        status="COMPLETED",
                        checkpoint="SAMPLE_BATCH_COMMITTED",
                        result={"batch_id": batch_id, **stats},
                    )
            except Exception as exc:
                self._mark_intent_failed(intent_id, "SAMPLE_BATCH_FAILED", exc)
                raise
        return {
            "batch_id": batch_id,
            "accepted": len(samples),
            **stats,
            "intent_id": intent_id,
        }

    def load_snapshot(self, raw: Any) -> dict[str, Any]:
        snapshot = normalize_snapshot(raw)
        version = int(snapshot["version"])
        with self.writer_lock:
            active_version = self.repo.active_snapshot_version()
            if active_version is not None:
                previous = self.repo.get_snapshot(active_version)
                self._assert_snapshot_monotonic(previous, snapshot)
            if self.repo.snapshot_exists(version):
                raise invalid_snapshot(
                    "snapshot.version already exists and cannot be loaded twice",
                    version=version,
                )
            intent_id = self.repo.start_intent(
                kind="SNAPSHOT_LOAD",
                session_id=None,
                request={"version": version, "generated_at": snapshot["generated_at"]},
            )
            self.crash.hit("SNAPSHOT_AFTER_INTENT")
            try:
                with self.db.write_transaction() as conn:
                    active_now = self.repo.active_snapshot_version(conn)
                    if active_now is not None:
                        previous_now = self.repo.get_snapshot(active_now, conn)
                        self._assert_snapshot_monotonic(previous_now, snapshot)
                    if self.repo.snapshot_exists(version, conn):
                        raise invalid_snapshot(
                            "snapshot.version already exists and cannot be loaded twice",
                            version=version,
                        )
                    self.repo.insert_snapshot(conn, snapshot)
                    self.repo.update_intent(
                        conn,
                        intent_id,
                        status="COMPLETED",
                        checkpoint="SNAPSHOT_ATOMICALLY_REPLACED",
                        result={"active_version": version, "retired_version": active_now},
                    )
            except Exception as exc:
                self._mark_intent_failed(intent_id, "SNAPSHOT_LOAD_FAILED", exc)
                raise
        return {
            "active_version": version,
            "previous_active_version": active_version,
            "generated_at": snapshot["generated_at"],
            "digest": snapshot["digest"],
            "summary": snapshot["summary"],
            "intent_id": intent_id,
        }

    def _assert_snapshot_monotonic(
        self, previous: dict[str, Any] | None, snapshot: dict[str, Any]
    ) -> None:
        if previous is None:
            return
        previous_version = int(previous["version"])
        new_version = int(snapshot["version"])
        if new_version <= previous_version:
            raise invalid_snapshot(
                "snapshot.version must be strictly greater than the active version",
                active_version=previous_version,
                supplied_version=new_version,
            )
        previous_time = self._parse_time(previous["generated_at"])
        new_time = self._parse_time(snapshot["generated_at"])
        if new_time <= previous_time:
            raise invalid_snapshot(
                "snapshot.generated_at must increase together with snapshot.version",
                active_generated_at=previous["generated_at"],
                supplied_generated_at=snapshot["generated_at"],
            )

    @staticmethod
    def _parse_time(value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    def _mark_intent_failed(self, intent_id: str, checkpoint: str, exc: BaseException) -> None:
        try:
            with self.db.write_transaction() as conn:
                self.repo.update_intent(
                    conn,
                    intent_id,
                    status="FAILED",
                    checkpoint=checkpoint,
                    result={
                        "error_code": getattr(exc, "code", "E_INTERNAL_ERROR"),
                        "message": str(exc),
                    },
                )
        except Exception:
            # The original failure is more useful to the caller. A pending intent is
            # deliberately left for startup recovery if this bookkeeping write fails.
            pass

    def create_session(self, *, session_id: str, sample_id: str) -> dict[str, Any]:
        if not isinstance(session_id, str) or not session_id.strip():
            raise invalid_sample("session_id must be a non-empty string")
        if not isinstance(sample_id, str) or not sample_id.strip():
            raise invalid_sample("sample_id must be a non-empty string")
        session_id = session_id.strip()
        sample_id = sample_id.strip()
        with self.writer_lock:
            existing = self.repo.get_session(session_id)
            if existing is not None:
                if existing["sample_id"] != sample_id:
                    raise BusinessError(
                        "E_SESSION_CONFLICT",
                        "The session_id is already bound to a different sample.",
                        {"session_id": session_id, "existing_sample_id": existing["sample_id"]},
                    )
                return {"session": self._session_view(session_id), "created": False}
            if self.repo.get_sample(sample_id) is None:
                raise invalid_sample("sample_id was not found", sample_id=sample_id)
            intent_id = self.repo.start_intent(
                kind="SESSION_CREATE",
                session_id=session_id,
                request={"session_id": session_id, "sample_id": sample_id},
            )
            self.crash.hit("SESSION_AFTER_INTENT")
            try:
                with self.db.write_transaction() as conn:
                    session, created = self.repo.create_session(
                        conn, session_id=session_id, sample_id=sample_id
                    )
                    self.repo.update_intent(
                        conn,
                        intent_id,
                        status="COMPLETED",
                        checkpoint="SESSION_CREATED",
                        result={"created": created},
                    )
            except Exception as exc:
                self._mark_intent_failed(intent_id, "SESSION_CREATE_FAILED", exc)
                raise
        return {"session": self._session_view(session_id), "created": created}

    # ------------------------------------------------------------------ diagnosis
    def diagnose(
        self, session_id: str, *, requested_snapshot_version: int | None = None
    ) -> dict[str, Any]:
        with self._session_guard(session_id):
            with self.writer_lock:
                session = self.repo.require_session(session_id)
                status = session["status"]
                if status in {SessionStatus.SUGGESTED, SessionStatus.VERIFIED}:
                    self._validate_reused_session_snapshot(session, requested_snapshot_version)
                    return {
                        "session": self._session_view(session_id),
                        "reused": True,
                        "message": "Diagnosis already completed for this session; persisted result reused.",
                    }
                if status == SessionStatus.FAILED:
                    raise invalid_transition(
                        "A FAILED session is terminal and cannot re-enter ANALYZING.",
                        session_id=session_id,
                        current_status=status,
                    )
                if status == SessionStatus.ANALYZING:
                    raise invalid_transition(
                        "The session is ANALYZING; recovery must finish before a new diagnosis can start.",
                        session_id=session_id,
                        current_status=status,
                    )

                snapshot = self._snapshot_for_new_analysis(requested_snapshot_version)
                sample_record = self.repo.get_sample(session["sample_id"])
                if sample_record is None:
                    raise invalid_sample(
                        "The session's sample no longer exists.", sample_id=session["sample_id"]
                    )
                sample = sample_record["normalized"]
                snapshot_version = int(snapshot["version"])
                self.crash.hit("ANALYZE_AFTER_SNAPSHOT_SELECTED")
                intent_id = self.repo.start_intent(
                    kind="ANALYZE",
                    session_id=session_id,
                    request={
                        "sample_id": session["sample_id"],
                        "snapshot_version": snapshot_version,
                    },
                )
                self.crash.hit("ANALYZE_AFTER_INTENT")
                try:
                    with self.db.write_transaction() as conn:
                        self.repo.transition_session(
                            conn,
                            session_id=session_id,
                            target=SessionStatus.ANALYZING,
                            expected=SessionStatus.CREATED,
                            updates={"snapshot_version": snapshot_version},
                            reason="DIAGNOSIS_STARTED",
                            intent_id=intent_id,
                        )
                        self.repo.update_intent(
                            conn,
                            intent_id,
                            status="PENDING",
                            checkpoint="SESSION_ANALYZING",
                        )
                    self.crash.hit("ANALYZE_AFTER_ANALYZING")
                    plan = self.planner.plan(
                        session_id=session_id, sample=sample, snapshot=snapshot
                    )
                    self.crash.hit("ANALYZE_AFTER_PLAN")
                    with self.db.write_transaction() as conn:
                        self.repo.insert_suggestions(conn, plan.suggestions)
                        self.repo.transition_session(
                            conn,
                            session_id=session_id,
                            target=SessionStatus.SUGGESTED,
                            expected=SessionStatus.ANALYZING,
                            updates={
                                "is_slow": 1 if plan.is_slow else 0,
                                "analysis_json": canonical_json(plan.analysis),
                                "last_error_code": None,
                                "last_error_message": None,
                            },
                            reason="DIAGNOSIS_COMPLETED",
                            intent_id=intent_id,
                        )
                        self.repo.update_intent(
                            conn,
                            intent_id,
                            status="COMPLETED",
                            checkpoint="SUGGESTIONS_AND_STATUS_COMMITTED",
                            result={
                                "is_slow": plan.is_slow,
                                "suggestion_count": len(plan.suggestions),
                            },
                        )
                except BusinessError as exc:
                    self._finalize_analysis_failure(session_id, intent_id, exc)
                    raise
                except Exception as exc:
                    failure = BusinessError(
                        "E_INTERNAL_ERROR",
                        "Diagnosis failed before a persisted suggestion set was committed.",
                        {"session_id": session_id, "cause": str(exc)},
                    )
                    self._finalize_analysis_failure(session_id, intent_id, failure)
                    raise failure from exc
        return {
            "session": self._session_view(session_id),
            "reused": False,
            "message": "Diagnosis committed atomically with its rewrite suggestions.",
        }

    def _validate_reused_session_snapshot(
        self, session: dict[str, Any], requested_snapshot_version: int | None
    ) -> None:
        if requested_snapshot_version is None:
            return
        if not isinstance(requested_snapshot_version, int) or isinstance(
            requested_snapshot_version, bool
        ):
            raise invalid_snapshot("snapshot_version must be an integer")
        bound = session["snapshot_version"]
        active = self.repo.active_snapshot_version()
        if requested_snapshot_version == bound:
            return
        if requested_snapshot_version != active and self.repo.snapshot_exists(
            requested_snapshot_version
        ):
            raise BusinessError(
                "E_SNAPSHOT_RETIRED",
                "The requested snapshot version has been retired and cannot reinterpret a session.",
                {
                    "session_id": session["session_id"],
                    "requested_snapshot_version": requested_snapshot_version,
                    "bound_snapshot_version": bound,
                    "active_snapshot_version": active,
                },
            )
        if not self.repo.snapshot_exists(requested_snapshot_version):
            raise invalid_snapshot(
                "The requested snapshot version does not exist.",
                requested_snapshot_version=requested_snapshot_version,
            )
        raise invalid_transition(
            "A completed session is permanently bound to its analysis snapshot version.",
            session_id=session["session_id"],
            bound_snapshot_version=bound,
            requested_snapshot_version=requested_snapshot_version,
        )

    def _snapshot_for_new_analysis(self, requested: int | None) -> dict[str, Any]:
        if requested is not None:
            if not isinstance(requested, int) or isinstance(requested, bool):
                raise invalid_snapshot("snapshot_version must be an integer")
            active = self.repo.active_snapshot_version()
            if requested == active:
                snapshot = self.repo.get_snapshot(requested)
                if snapshot is None:
                    raise invalid_snapshot("Active snapshot is missing its payload.")
                return snapshot
            if self.repo.snapshot_exists(requested):
                raise BusinessError(
                    "E_SNAPSHOT_RETIRED",
                    "The requested snapshot version has been replaced by a newer active snapshot.",
                    {
                        "requested_snapshot_version": requested,
                        "active_snapshot_version": active,
                    },
                )
            raise invalid_snapshot(
                "The requested snapshot version does not exist.",
                requested_snapshot_version=requested,
            )
        snapshot = self.repo.get_active_snapshot()
        if snapshot is None:
            raise invalid_snapshot(
                "No statistics snapshot is loaded; diagnosis suggestions are prohibited."
            )
        return snapshot

    def _finalize_analysis_failure(
        self, session_id: str, intent_id: str, error: BusinessError
    ) -> None:
        try:
            with self.db.write_transaction() as conn:
                session = self.repo.require_session(session_id, conn)
                if session["status"] == SessionStatus.ANALYZING:
                    self.repo.transition_session(
                        conn,
                        session_id=session_id,
                        target=SessionStatus.FAILED,
                        expected=SessionStatus.ANALYZING,
                        updates={
                            "last_error_code": error.code,
                            "last_error_message": error.message,
                        },
                        reason="DIAGNOSIS_FAILED",
                        intent_id=intent_id,
                    )
                self.repo.update_intent(
                    conn,
                    intent_id,
                    status="FAILED",
                    checkpoint="DIAGNOSIS_FAILED",
                    result={"error_code": error.code, "message": error.message},
                )
        except Exception:
            pass

    # ------------------------------------------------------------------ read APIs
    def get_session(self, session_id: str) -> dict[str, Any]:
        return self._session_view(session_id)

    def get_suggestions(self, session_id: str) -> dict[str, Any]:
        session = self._session_view(session_id)
        if session["status"] in {SessionStatus.CREATED, SessionStatus.ANALYZING}:
            raise invalid_transition(
                "Suggestions are not readable until diagnosis commits SUGGESTED.",
                session_id=session_id,
                current_status=session["status"],
            )
        if session["status"] == SessionStatus.SUGGESTED and session["is_slow"]:
            if not session["suggestions"]:
                raise BusinessError(
                    "E_INTERNAL_ERROR",
                    "Persisted invariant violation: SUGGESTED has no suggestions.",
                    {"session_id": session_id},
                )
        return {
            "session_id": session_id,
            "status": session["status"],
            "snapshot_version": session["snapshot_version"],
            "analysis": session["analysis"],
            "suggestions": session["suggestions"],
            "verifications": session["verifications"],
        }

    # ------------------------------------------------------------------ verification
    def verify(
        self, session_id: str, *, suggestion_id: str | None = None
    ) -> dict[str, Any]:
        with self._session_guard(session_id):
            with self.writer_lock:
                session = self.repo.require_session(session_id)
                if session["status"] == SessionStatus.VERIFIED:
                    record = self._reuse_verified_record(session_id, suggestion_id)
                    return {
                        "session": self._session_view(session_id),
                        "verification": record,
                        "reused": True,
                        "message": "The session is terminal VERIFIED; persisted verification reused.",
                    }
                if session["status"] == SessionStatus.FAILED:
                    raise invalid_transition(
                        "A FAILED session is terminal and cannot re-enter ANALYZING for verification.",
                        session_id=session_id,
                        current_status=session["status"],
                    )
                if session["status"] in {SessionStatus.CREATED, SessionStatus.ANALYZING}:
                    raise invalid_transition(
                        "Only a SUGGESTED session with persisted suggestions can be verified.",
                        session_id=session_id,
                        current_status=session["status"],
                    )

                suggestions = self.repo.get_suggestions(session_id)
                if not suggestions:
                    raise invalid_transition(
                        "This session has no rewrite suggestions to verify.",
                        session_id=session_id,
                        current_status=session["status"],
                    )
                suggestion = self._select_suggestion(suggestions, suggestion_id)
                if suggestion["snapshot_version"] != session["snapshot_version"]:
                    raise BusinessError(
                        "E_SNAPSHOT_INVALID",
                        "Suggestion and session snapshot bindings are inconsistent.",
                        {
                            "session_id": session_id,
                            "session_snapshot_version": session["snapshot_version"],
                            "suggestion_snapshot_version": suggestion["snapshot_version"],
                        },
                    )
                snapshot = self.repo.get_snapshot(session["snapshot_version"])
                if snapshot is None:
                    raise invalid_snapshot(
                        "The session's bound snapshot payload is unavailable; verification is prohibited.",
                        snapshot_version=session["snapshot_version"],
                    )
                intent_id = self.repo.start_intent(
                    kind="VERIFY",
                    session_id=session_id,
                    request={
                        "suggestion_id": suggestion["suggestion_id"],
                        "snapshot_version": session["snapshot_version"],
                    },
                )
                self.crash.hit("VERIFY_AFTER_INTENT")
                with self.db.write_transaction() as conn:
                    self.repo.transition_session(
                        conn,
                        session_id=session_id,
                        target=SessionStatus.ANALYZING,
                        expected=SessionStatus.SUGGESTED,
                        reason="VERIFICATION_STARTED",
                        intent_id=intent_id,
                    )
                    self.repo.update_intent(
                        conn,
                        intent_id,
                        status="PENDING",
                        checkpoint="SESSION_VERIFYING",
                    )
                self.crash.hit("VERIFY_AFTER_ANALYZING")
                try:
                    result = self._verifier(
                        sample_database_path=self.config.sample_database_path,
                        original_sql=suggestion["original_sql"],
                        suggestion=suggestion,
                        snapshot=snapshot,
                        config=self.config,
                    )
                except VerificationExecutionError as exc:
                    self._finalize_verification_failure_without_result(
                        session_id=session_id,
                        suggestion=suggestion,
                        intent_id=intent_id,
                        code=exc.code,
                        message=exc.message,
                        details=exc.details,
                    )
                    raise exc.as_business_error() from exc
                except Exception as exc:
                    self._finalize_verification_failure_without_result(
                        session_id=session_id,
                        suggestion=suggestion,
                        intent_id=intent_id,
                        code="E_VERIFY_ERROR",
                        message="Verification executor failed unexpectedly.",
                        details={"exception": str(exc)},
                    )
                    raise BusinessError(
                        "E_VERIFY_ERROR",
                        "Verification executor failed unexpectedly.",
                        {"session_id": session_id, "exception": str(exc)},
                    ) from exc

                self.crash.hit("VERIFY_AFTER_EXECUTION_BEFORE_COMMIT")
                record = self._persist_verification_result(
                    session_id=session_id,
                    suggestion=suggestion,
                    intent_id=intent_id,
                    result=result,
                )
                if result["outcome"] != "VERIFIED":
                    raise BusinessError(
                        "E_VERIFY_MISMATCH"
                        if result.get("error_code") == "E_VERIFY_MISMATCH"
                        else "E_VERIFY_ERROR",
                        result.get("message") or "Rewrite verification failed.",
                        {
                            "session_id": session_id,
                            "suggestion_id": suggestion["suggestion_id"],
                            "verification": record,
                            "original_cost": result.get("original_cost", {}),
                            "rewritten_cost": result.get("rewritten_cost", {}),
                            "original_result_digest": result.get("original_result_digest"),
                            "rewritten_result_digest": result.get("rewritten_result_digest"),
                            "original_row_count": result.get("original_row_count"),
                            "rewritten_row_count": result.get("rewritten_row_count"),
                            "resources": result.get("resources", {}),
                        },
                    )
        return {
            "session": self._session_view(session_id),
            "verification": record,
            "reused": False,
            "message": "Rewrite result set is consistent; session is VERIFIED.",
        }

    def _select_suggestion(
        self, suggestions: list[dict[str, Any]], suggestion_id: str | None
    ) -> dict[str, Any]:
        if suggestion_id is None:
            return suggestions[0]
        for suggestion in suggestions:
            if suggestion["suggestion_id"] == suggestion_id:
                return suggestion
        raise invalid_transition(
            "The requested suggestion does not belong to this session.",
            suggestion_id=suggestion_id,
        )

    def _reuse_verified_record(
        self, session_id: str, suggestion_id: str | None
    ) -> dict[str, Any]:
        if suggestion_id is not None:
            record = self.repo.get_verification_for_suggestion(suggestion_id)
            if record is not None and record["session_id"] == session_id:
                return record
            raise invalid_transition(
                "The requested suggestion has no persisted successful verification.",
                session_id=session_id,
                suggestion_id=suggestion_id,
            )
        for record in self.repo.list_verifications(session_id):
            if record["outcome"] == "VERIFIED":
                return record
        raise BusinessError(
            "E_INTERNAL_ERROR",
            "Persisted invariant violation: VERIFIED session has no verification record.",
            {"session_id": session_id},
        )

    def _verification_record(
        self,
        *,
        session_id: str,
        suggestion: dict[str, Any],
        result: dict[str, Any],
    ) -> dict[str, Any]:
        extra_details = dict(result.get("details") or {})
        extra_details.update(
            {
                "message": result.get("message"),
                "cost_improved": result.get("cost_improved"),
                "original_columns": result.get("original_columns"),
                "rewritten_columns": result.get("rewritten_columns"),
                "original_preview": result.get("original_preview"),
                "rewritten_preview": result.get("rewritten_preview"),
                "resources": result.get("resources", {}),
            }
        )
        return {
            "verification_id": "ver_" + uuid.uuid4().hex,
            "session_id": session_id,
            "suggestion_id": suggestion["suggestion_id"],
            "outcome": result["outcome"],
            "original_cost": result.get("original_cost", {}),
            "rewritten_cost": result.get("rewritten_cost", {}),
            "original_result_digest": result.get("original_result_digest"),
            "rewritten_result_digest": result.get("rewritten_result_digest"),
            "original_row_count": result.get("original_row_count"),
            "rewritten_row_count": result.get("rewritten_row_count"),
            "error_code": result.get("error_code"),
            "details": extra_details,
        }

    def _persist_verification_result(
        self,
        *,
        session_id: str,
        suggestion: dict[str, Any],
        intent_id: str,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        record = self._verification_record(
            session_id=session_id, suggestion=suggestion, result=result
        )
        with self.db.write_transaction() as conn:
            persisted = self.repo.insert_verification(conn, record)
            if result["outcome"] == "VERIFIED":
                self.repo.transition_session(
                    conn,
                    session_id=session_id,
                    target=SessionStatus.VERIFIED,
                    expected=SessionStatus.ANALYZING,
                    updates={
                        "last_error_code": None,
                        "last_error_message": None,
                    },
                    reason="REWRITE_VERIFIED",
                    intent_id=intent_id,
                )
                self.repo.set_suggestion_verification_status(
                    conn, suggestion["suggestion_id"], "VERIFIED"
                )
                self.repo.update_intent(
                    conn,
                    intent_id,
                    status="COMPLETED",
                    checkpoint="VERIFICATION_AND_SESSION_VERIFIED_COMMITTED",
                    result={
                        "outcome": "VERIFIED",
                        "verification_id": persisted["verification_id"],
                    },
                )
            else:
                self.repo.set_suggestion_verification_status(
                    conn, suggestion["suggestion_id"], "FAILED"
                )
                self.repo.transition_session(
                    conn,
                    session_id=session_id,
                    target=SessionStatus.FAILED,
                    expected=SessionStatus.ANALYZING,
                    updates={
                        "last_error_code": result.get("error_code"),
                        "last_error_message": result.get("message"),
                    },
                    reason="REWRITE_VERIFICATION_FAILED",
                    intent_id=intent_id,
                )
                self.repo.update_intent(
                    conn,
                    intent_id,
                    status="COMPLETED",
                    checkpoint="VERIFICATION_FAILURE_COMMITTED",
                    result={
                        "outcome": "FAILED",
                        "error_code": result.get("error_code"),
                        "verification_id": persisted["verification_id"],
                    },
                )
        return persisted

    def _finalize_verification_failure_without_result(
        self,
        *,
        session_id: str,
        suggestion: dict[str, Any],
        intent_id: str,
        code: str,
        message: str,
        details: dict[str, Any],
    ) -> None:
        result = {
            "outcome": "FAILED",
            "error_code": code,
            "message": message,
            "original_cost": {},
            "rewritten_cost": {},
            "original_result_digest": None,
            "rewritten_result_digest": None,
            "original_row_count": None,
            "rewritten_row_count": None,
            "cost_improved": None,
            "details": {"error_details": details},
            "resources": {},
        }
        try:
            self._persist_verification_result(
                session_id=session_id,
                suggestion=suggestion,
                intent_id=intent_id,
                result=result,
            )
        except Exception:
            try:
                with self.db.write_transaction() as conn:
                    current = self.repo.require_session(session_id, conn)
                    if current["status"] == SessionStatus.ANALYZING:
                        self.repo.transition_session(
                            conn,
                            session_id=session_id,
                            target=SessionStatus.FAILED,
                            expected=SessionStatus.ANALYZING,
                            updates={
                                "last_error_code": code,
                                "last_error_message": message,
                            },
                            reason="VERIFICATION_FAILED_WITHOUT_RECORD",
                            intent_id=intent_id,
                        )
                    self.repo.update_intent(
                        conn,
                        intent_id,
                        status="FAILED",
                        checkpoint="VERIFICATION_FAILURE_PERSIST_FAILED",
                        result={"error_code": code, "message": message},
                    )
            except Exception:
                pass

    # ------------------------------------------------------------------ recovery
    def recover_incomplete(self) -> dict[str, Any]:
        with self.writer_lock:
            pending = self.repo.pending_intents()
            recoveries: list[dict[str, Any]] = []
            for intent in pending:
                entry = {
                    "intent_id": intent["intent_id"],
                    "session_id": intent["session_id"],
                    "kind": intent["kind"],
                    "checkpoint": intent["checkpoint"],
                    "previous_status": None,
                    "final_status": None,
                    "action": None,
                    "reason": None,
                }
                try:
                    with self.db.write_transaction() as conn:
                        session = None
                        if intent["session_id"]:
                            session = self.repo.get_session(intent["session_id"], conn)
                            if session is not None:
                                entry["previous_status"] = session["status"]
                        if intent["kind"] == "ANALYZE":
                            if session is not None and session["status"] == SessionStatus.ANALYZING:
                                self.repo.transition_session(
                                    conn,
                                    session_id=intent["session_id"],
                                    target=SessionStatus.CREATED,
                                    expected=SessionStatus.ANALYZING,
                                    updates={
                                        "snapshot_version": None,
                                        "is_slow": None,
                                        "analysis_json": None,
                                        "last_error_code": "E_RECOVERED_INCOMPLETE_ANALYSIS",
                                        "last_error_message": (
                                            "Restart recovery discarded an interrupted ANALYZING run; "
                                            "no suggestion was marked as derived from it."
                                        ),
                                    },
                                    reason="RECOVERY_ANALYSIS_DISCARDED",
                                    intent_id=intent["intent_id"],
                                )
                                entry["action"] = "SESSION_RESTORED_TO_CREATED"
                                entry["reason"] = "ANALYZING had a durable pending intent without committed suggestions."
                            elif session is not None and session["status"] == SessionStatus.CREATED:
                                entry["action"] = "INTENT_DISCARDED_SESSION_REMAINS_CREATED"
                                entry["reason"] = "The process stopped before the CREATED -> ANALYZING commit."
                            else:
                                entry["action"] = "INTENT_SUPERSEDED"
                                entry["reason"] = "Committed session facts supersede the stale analysis intent."
                            self.repo.update_intent(
                                conn,
                                intent["intent_id"],
                                status="RECOVERED",
                                checkpoint="ANALYSIS_RECOVERY_COMMITTED",
                                result={"action": entry["action"], "reason": entry["reason"]},
                            )
                        elif intent["kind"] == "VERIFY":
                            if session is not None and session["status"] in {
                                SessionStatus.ANALYZING,
                                SessionStatus.SUGGESTED,
                            }:
                                self.repo.transition_session(
                                    conn,
                                    session_id=intent["session_id"],
                                    target=SessionStatus.FAILED,
                                    expected=session["status"],
                                    updates={
                                        "last_error_code": "E_RECOVERED_INCOMPLETE_VERIFICATION",
                                        "last_error_message": (
                                            "Restart recovery failed the session because verification "
                                            "was interrupted before a durable outcome existed."
                                        ),
                                    },
                                    reason="RECOVERY_VERIFICATION_INTERRUPTED",
                                    intent_id=intent["intent_id"],
                                )
                                entry["action"] = "SESSION_FAILED_NOT_VERIFIED"
                                entry["reason"] = (
                                    "A pending verification intent is never treated as success; "
                                    "only a committed verification record can produce VERIFIED."
                                )
                            else:
                                entry["action"] = "INTENT_SUPERSEDED_OR_FAILED_SESSION_UNCHANGED"
                                entry["reason"] = (
                                    "No SUGGESTED/ANALYZING session needed a verification rollback."
                                )
                            self.repo.update_intent(
                                conn,
                                intent["intent_id"],
                                status="RECOVERED",
                                checkpoint="VERIFICATION_RECOVERY_COMMITTED",
                                result={"action": entry["action"], "reason": entry["reason"]},
                            )
                        else:
                            entry["action"] = "INTENT_ABANDONED"
                            entry["reason"] = (
                                "Non-session data writes are atomic; no partial row was committed."
                            )
                            self.repo.update_intent(
                                conn,
                                intent["intent_id"],
                                status="ABANDONED",
                                checkpoint="NON_SESSION_WRITE_ABANDONED",
                                result={"action": entry["action"]},
                            )
                    final_session = (
                        self.repo.get_session(intent["session_id"])
                        if intent["session_id"]
                        else None
                    )
                    entry["final_status"] = None if final_session is None else final_session["status"]
                except Exception as exc:
                    entry["action"] = "RECOVERY_ERROR"
                    entry["reason"] = str(exc)
                recoveries.append(entry)

            facts = self.repo.recovery_facts()
            payload = {
                "generated_at": self.clock.now(),
                "facts_only": True,
                "source": "Committed SQLite rows only: operation_intents, sessions, session_events, suggestions, verification_records.",
                "pending_intents_scanned": len(pending),
                "recoveries": recoveries,
                "facts": facts,
                "guarantees": {
                    "interrupted_analysis_restored_to_created": all(
                        item["final_status"] != "ANALYZING" for item in recoveries
                    ),
                    "interrupted_verification_marked_verified": any(
                        item["kind"] == "VERIFY" and item["final_status"] == "VERIFIED"
                        for item in recoveries
                    ),
                    "verified_sessions_without_record": facts["invariants"][
                        "verified_without_verification_record"
                    ],
                    "suggested_sessions_without_suggestions": facts["invariants"][
                        "suggested_without_suggestions"
                    ],
                },
                "notes": [
                    "Recovery never promotes an interrupted verification to VERIFIED.",
                    "No in-memory diagnosis or verification output is used in this report.",
                ],
            }
            report = self.repo.store_recovery_report(payload)
            self.last_recovery = report
            return report

    def get_recovery_report(self) -> dict[str, Any]:
        report = self.repo.latest_recovery_report()
        if report is not None:
            return report
        facts = self.repo.recovery_facts()
        return {
            "report_id": None,
            "generated_at": self.clock.now(),
            "payload": {
                "generated_at": self.clock.now(),
                "facts_only": True,
                "source": "Committed SQLite rows only.",
                "pending_intents_scanned": 0,
                "recoveries": [],
                "facts": facts,
                "notes": ["No persisted startup recovery report exists."],
            },
        }
