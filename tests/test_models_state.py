from __future__ import annotations

import pytest

from grad_fault_lab.config import LabConfig
from grad_fault_lab.errors import AppError, ErrorCode
from grad_fault_lab.models import InjectionSpec, classify_code
from grad_fault_lab.state_machine import BatchState, LEGAL_TRANSITIONS, StateTracker


def test_all_injection_types_parse_and_have_distinct_codes() -> None:
    from grad_fault_lab.models import FAULT_CODES, InjectionType

    assert {item.value for item in InjectionType} == {
        "connection_refused",
        "response_timeout",
        "content_truncated",
        "duplicate_submission",
        "order_mismatch",
    }
    codes = [FAULT_CODES[item].value for item in InjectionType]
    assert len(codes) == len(set(codes)) == 5


def test_unknown_node_is_injection_spec_invalid() -> None:
    config = LabConfig.load("config/default.json")
    spec = InjectionSpec.from_payload(
        {
            "type": "response_timeout",
            "target_node": "missing-node",
            "target_shard": 0,
            "round": 0,
        }
    )
    from grad_fault_lab.injection import validate_injection_against_config

    with pytest.raises(AppError) as raised:
        validate_injection_against_config(config, spec, 0)
    assert raised.value.code is ErrorCode.INJECTION_SPEC_INVALID


def test_unknown_injection_type_is_spec_invalid() -> None:
    with pytest.raises(AppError) as raised:
        InjectionSpec.from_payload(
            {
                "type": "made_up_fault",
                "target_node": "node-0",
                "target_shard": 0,
                "round": 0,
            }
        )
    assert raised.value.code is ErrorCode.INJECTION_SPEC_INVALID


def test_fault_classification_contract() -> None:
    assert classify_code(ErrorCode.FAULT_RESPONSE_TIMEOUT).value == "RETRYABLE"
    assert classify_code(ErrorCode.FAULT_CONNECTION_REFUSED).value == "RETRYABLE"
    assert classify_code(ErrorCode.FAULT_CONTENT_TRUNCATED).value == "FATAL"
    assert classify_code(ErrorCode.FAULT_DUPLICATE_SUBMISSION).value == "FATAL"
    assert classify_code(ErrorCode.FAULT_ORDER_MISMATCH).value == "FATAL"
    assert classify_code(ErrorCode.BLOCK_VALIDATION_FAILED).value == "FATAL"


def test_state_machine_legal_paths_and_invalid_no_mutation() -> None:
    tracker = StateTracker()
    tracker.transition(BatchState.RETRYING, reason="retry")
    tracker.transition(BatchState.DEGRADED, reason="exhausted")
    tracker.transition(BatchState.VERIFIED, reason="shortened aggregate")
    assert tracker.current is BatchState.VERIFIED
    assert len(tracker.history) == 3

    before_state = tracker.current
    before_history = list(tracker.history)
    with pytest.raises(AppError) as raised:
        tracker.transition(BatchState.RETRYING, reason="illegal")
    assert raised.value.code is ErrorCode.INVALID_TRANSITION
    assert tracker.current is before_state
    assert tracker.history == before_history


def test_every_nonterminal_state_has_legal_exit() -> None:
    for state in (BatchState.INJECTING, BatchState.RETRYING, BatchState.DEGRADED):
        assert LEGAL_TRANSITIONS[state]
    assert not LEGAL_TRANSITIONS[BatchState.VERIFIED]
    assert not LEGAL_TRANSITIONS[BatchState.ABORTED]


def test_error_envelope_shape_for_every_business_code() -> None:
    for code in ErrorCode:
        error = AppError(code, "test", {"probe": True})
        envelope = error.to_envelope(batch_id="batch-probe")
        assert set(envelope) == {"ok", "error"}
        assert envelope["ok"] is False
        assert envelope["error"]["code"] == code.value
        assert envelope["error"]["details"]["batch_id"] == "batch-probe"

