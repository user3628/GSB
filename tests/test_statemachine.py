import pytest

from fiw.errors import BizError, E_INVALID_TRANSITION
from fiw.models import (
    STATUS_ABORTED,
    STATUS_COMPLETED,
    STATUS_PAUSED,
    STATUS_PLANNED,
    STATUS_RUNNING,
)
from fiw.statemachine import check_transition


def test_legal_transitions():
    assert check_transition(STATUS_PLANNED, STATUS_RUNNING) == STATUS_RUNNING
    assert check_transition(STATUS_RUNNING, STATUS_PAUSED) == STATUS_PAUSED
    assert check_transition(STATUS_PAUSED, STATUS_RUNNING) == STATUS_RUNNING
    assert check_transition(STATUS_RUNNING, STATUS_COMPLETED) == STATUS_COMPLETED
    assert check_transition(STATUS_RUNNING, STATUS_ABORTED) == STATUS_ABORTED
    assert check_transition(STATUS_PLANNED, STATUS_ABORTED) == STATUS_ABORTED


def test_illegal_transitions_rejected():
    for current, target in [
        (STATUS_PLANNED, STATUS_COMPLETED),
        (STATUS_PLANNED, STATUS_PAUSED),
        (STATUS_COMPLETED, STATUS_RUNNING),
        (STATUS_ABORTED, STATUS_RUNNING),
        (STATUS_PAUSED, STATUS_COMPLETED),
    ]:
        with pytest.raises(BizError) as exc:
            check_transition(current, target)
        assert exc.value.code == E_INVALID_TRANSITION


def test_illegal_transition_does_not_modify_persisted_state(loaded_engine):
    engine = loaded_engine
    with pytest.raises(BizError) as exc:
        engine.transition("plan-test", STATUS_COMPLETED)
    assert exc.value.code == E_INVALID_TRANSITION
    assert engine.get_plan("plan-test")["status"] == STATUS_PLANNED


def test_engine_transition_flow(loaded_engine):
    engine = loaded_engine
    assert engine.transition("plan-test", STATUS_RUNNING)["status"] == STATUS_RUNNING
    assert engine.transition("plan-test", STATUS_PAUSED)["status"] == STATUS_PAUSED
    assert engine.transition("plan-test", STATUS_RUNNING)["status"] == STATUS_RUNNING
    assert engine.transition("plan-test", STATUS_ABORTED)["status"] == STATUS_ABORTED
    with pytest.raises(BizError):
        engine.transition("plan-test", STATUS_RUNNING)
