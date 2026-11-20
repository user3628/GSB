from __future__ import annotations

import sqlite3

import pytest

from capacity_workbench.db import Store
from capacity_workbench.errors import InvalidTransition
from capacity_workbench.state_machine import RoundStatus, validate_transition


@pytest.mark.parametrize(
    ("current", "target"),
    [
        (RoundStatus.PENDING, RoundStatus.SAMPLING),
        (RoundStatus.PENDING, RoundStatus.ABORTED),
        (RoundStatus.SAMPLING, RoundStatus.CONVERGED),
        (RoundStatus.SAMPLING, RoundStatus.ABORTED),
    ],
)
def test_legal_transitions(current: RoundStatus, target: RoundStatus) -> None:
    validate_transition(current, target)


@pytest.mark.parametrize(
    ("current", "target"),
    [
        ("PENDING", "CONVERGED"),
        ("CONVERGED", "SAMPLING"),
        ("ABORTED", "SAMPLING"),
        ("SAMPLING", "PENDING"),
        ("SAMPLING", "SAMPLING"),
    ],
)
def test_illegal_transitions_do_not_modify_persisted_fields(
    db_path, base_plan: dict, current: str, target: str
) -> None:
    store = Store(db_path)
    store.init_schema()
    plan = store.insert_plan(base_plan)
    round_record = store.create_round(
        plan_id=plan.id,
        stage_index=0,
        attempt=1,
        concurrency=1,
        duration_seconds=1,
        origin="sequence",
    )
    if current == "SAMPLING":
        store.transition_round(round_record.id, "SAMPLING", reason="test_setup")
    elif current == "CONVERGED":
        store.transition_round(round_record.id, "SAMPLING", reason="test_setup")
        store.transition_round(round_record.id, "CONVERGED", reason="test_setup")
    elif current == "ABORTED":
        store.transition_round(round_record.id, "ABORTED", reason="test_setup")
    before = store.get_round(round_record.id)
    with pytest.raises(InvalidTransition) as error:
        store.transition_round(round_record.id, target)
    assert error.value.code == "E_INVALID_TRANSITION"
    after = store.get_round(round_record.id)
    assert after == before
    with sqlite3.connect(db_path) as connection:
        count = connection.execute(
            "SELECT COUNT(*) FROM round_transitions WHERE round_id = ? AND reason = 'manual'",
            (round_record.id,),
        ).fetchone()[0]
    assert count == 0


def test_start_round_writes_an_open_intent(db_path, base_plan: dict) -> None:
    store = Store(db_path)
    store.init_schema()
    plan = store.insert_plan(base_plan)
    round_record = store.create_round(
        plan_id=plan.id,
        stage_index=0,
        attempt=1,
        concurrency=1,
        duration_seconds=1,
        origin="sequence",
    )
    intent_id = store.start_round(round_record.id)
    assert intent_id.startswith("intent_")
    with sqlite3.connect(db_path) as connection:
        intent = connection.execute(
            "SELECT status, round_id FROM intents WHERE id = ?", (intent_id,)
        ).fetchone()
    assert intent == ("OPEN", round_record.id)
    assert store.get_round(round_record.id).status == "SAMPLING"

