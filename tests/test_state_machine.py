from __future__ import annotations

import sqlite3

import pytest

from slowquery.errors import BusinessError
from slowquery.service import SlowQueryService
from slowquery.state_machine import (
    SessionStatus,
    can_transition,
    require_transition,
)


def test_legal_and_illegal_transition_table() -> None:
    assert can_transition(SessionStatus.CREATED, SessionStatus.ANALYZING)
    assert can_transition(SessionStatus.ANALYZING, SessionStatus.SUGGESTED)
    assert can_transition(SessionStatus.SUGGESTED, SessionStatus.ANALYZING)
    assert can_transition(SessionStatus.ANALYZING, SessionStatus.VERIFIED)
    assert can_transition(SessionStatus.ANALYZING, SessionStatus.CREATED)
    assert not can_transition(SessionStatus.VERIFIED, SessionStatus.ANALYZING)
    assert not can_transition(SessionStatus.FAILED, SessionStatus.ANALYZING)
    with pytest.raises(BusinessError) as caught:
        require_transition(SessionStatus.VERIFIED, SessionStatus.ANALYZING)
    assert caught.value.code == "E_INVALID_TRANSITION"


def test_invalid_transition_does_not_modify_persisted_status(
    seeded_service: SlowQueryService,
) -> None:
    seeded_service.create_session(session_id="state-1", sample_id="sq-missing-index")
    with pytest.raises(BusinessError) as caught:
        with seeded_service.db.write_transaction() as conn:
            seeded_service.repo.transition_session(
                conn,
                session_id="state-1",
                target=SessionStatus.SUGGESTED,
                expected=SessionStatus.CREATED,
            )
    assert caught.value.code == "E_INVALID_TRANSITION"
    assert seeded_service.get_session("state-1")["status"] == "CREATED"


def test_database_trigger_blocks_terminal_reanalysis(
    seeded_service: SlowQueryService,
) -> None:
    seeded_service.create_session(session_id="state-2", sample_id="sq-missing-index")
    seeded_service.diagnose("state-2")
    seeded_service.verify("state-2")
    assert seeded_service.get_session("state-2")["status"] == "VERIFIED"
    with seeded_service.db.write_transaction() as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE sessions SET status = 'ANALYZING' WHERE session_id = 'state-2'"
            )
    assert seeded_service.get_session("state-2")["status"] == "VERIFIED"


def test_suggested_status_requires_suggestions_atomically(
    seeded_service: SlowQueryService,
) -> None:
    seeded_service.create_session(session_id="state-3", sample_id="sq-missing-index")
    with seeded_service.db.write_transaction() as conn:
        seeded_service.repo.transition_session(
            conn,
            session_id="state-3",
            target=SessionStatus.ANALYZING,
            expected=SessionStatus.CREATED,
        )
    with seeded_service.db.write_transaction() as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "UPDATE sessions SET status = 'SUGGESTED', is_slow = 1 WHERE session_id = 'state-3'"
            )
    assert seeded_service.get_session("state-3")["status"] == "ANALYZING"
    assert seeded_service.repo.get_suggestions("state-3") == []


def test_verified_status_requires_verification_record(seeded_service: SlowQueryService) -> None:
    seeded_service.create_session(session_id="state-4", sample_id="sq-missing-index")
    seeded_service.diagnose("state-4")
    with seeded_service.db.write_transaction() as conn:
        seeded_service.repo.transition_session(
            conn,
            session_id="state-4",
            target=SessionStatus.ANALYZING,
            expected=SessionStatus.SUGGESTED,
        )
    with seeded_service.db.write_transaction() as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE sessions SET status = 'VERIFIED' WHERE session_id = 'state-4'")
    assert seeded_service.get_session("state-4")["status"] == "ANALYZING"


def test_failed_session_cannot_reenter_analyzing(seeded_service: SlowQueryService) -> None:
    seeded_service.create_session(session_id="state-5", sample_id="sq-full-scan")
    seeded_service.diagnose("state-5")
    with pytest.raises(BusinessError) as verify_error:
        seeded_service.verify("state-5")
    assert verify_error.value.code == "E_VERIFY_MISMATCH"
    assert seeded_service.get_session("state-5")["status"] == "FAILED"
    with pytest.raises(BusinessError) as caught:
        seeded_service.diagnose("state-5")
    assert caught.value.code == "E_INVALID_TRANSITION"
    assert seeded_service.get_session("state-5")["status"] == "FAILED"


def test_verified_duplicate_diagnosis_reuses_without_analyzing(
    seeded_service: SlowQueryService,
) -> None:
    seeded_service.create_session(session_id="state-6", sample_id="sq-missing-index")
    first = seeded_service.diagnose("state-6")
    seeded_service.verify("state-6")
    trace_size = len(seeded_service.get_session("state-6")["trace"])
    second = seeded_service.diagnose("state-6")
    assert second["reused"] is True
    assert first["session"]["analysis"] == second["session"]["analysis"]
    assert seeded_service.get_session("state-6")["status"] == "VERIFIED"
    assert len(seeded_service.get_session("state-6")["trace"]) == trace_size
