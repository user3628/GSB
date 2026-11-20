import pytest

from logindex.errors import InvalidTransitionError
from logindex.models import BatchState, assert_transition


@pytest.mark.parametrize(
    "current,target",
    [
        (BatchState.PENDING, BatchState.INDEXING),
        (BatchState.INDEXING, BatchState.COMMITTED),
        (BatchState.PENDING, BatchState.ABORTED),
        (BatchState.INDEXING, BatchState.ABORTED),
        (BatchState.COMMITTED, BatchState.INVALIDATED),
    ],
)
def test_legal_transitions(current, target):
    assert_transition(current, target)


@pytest.mark.parametrize(
    "current,target",
    [
        (BatchState.COMMITTED, BatchState.PENDING),
        (BatchState.COMMITTED, BatchState.INDEXING),
        (BatchState.INVALIDATED, BatchState.COMMITTED),
        (BatchState.ABORTED, BatchState.PENDING),
        (BatchState.PENDING, BatchState.COMMITTED),
        (BatchState.PENDING, BatchState.INVALIDATED),
    ],
)
def test_illegal_transitions(current, target):
    with pytest.raises(InvalidTransitionError) as exc:
        assert_transition(current, target)
    assert exc.value.code == "E_INVALID_TRANSITION"


def test_recovery_rollback_only_allowed_for_recovery():
    with pytest.raises(InvalidTransitionError):
        assert_transition(BatchState.INDEXING, BatchState.PENDING)
    assert_transition(BatchState.INDEXING, BatchState.PENDING, recovery=True)
