import pytest

from app import statemachine as sm
from app.errors import BizError, E_INVALID_TRANSITION


def test_legal_transitions():
    sm.check_transition(sm.HEALTHY, sm.ISOLATED)
    sm.check_transition(sm.ISOLATED, sm.FAILED_OVER)
    sm.check_transition(sm.ISOLATED, sm.FAILED)
    sm.check_transition(sm.DEGRADED, sm.HEALTHY)
    sm.check_transition(sm.FAILED_OVER, sm.HEALTHY)
    sm.check_transition(sm.FAILED, sm.HEALTHY)
    sm.check_transition(sm.FAILED, sm.FAILED)  # no-op


def test_illegal_transitions_raise_and_persist_nothing():
    with pytest.raises(BizError) as exc:
        sm.check_transition(sm.FAILED, sm.ISOLATED)
    assert exc.value.code == E_INVALID_TRANSITION
    with pytest.raises(BizError):
        sm.check_transition(sm.FAILED_OVER, sm.ISOLATED)
    with pytest.raises(BizError):
        sm.check_transition(sm.HEALTHY, "BOGUS")
