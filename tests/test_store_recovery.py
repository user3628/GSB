from __future__ import annotations

import socket
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from grad_fault_lab.config import LabConfig
from grad_fault_lab.errors import AppError, ErrorCode
from grad_fault_lab.models import sha256_json
from grad_fault_lab.orchestrator import BatchOrchestrator, BatchRequest
from grad_fault_lab.state_machine import BatchState
from grad_fault_lab.store import Store


def _aggregation() -> dict:
    return {
        "status": "ABORTED",
        "degraded": False,
        "participant_nodes": [],
        "participant_blocks": 0,
        "omitted_nodes": [],
        "sample_weights": {},
        "tensor": None,
        "checksum": None,
        "error_bounds": None,
    }


def test_cost_recovery_marks_unfinished_intent_aborted(tmp_path) -> None:
    db_path = tmp_path / "recovery.sqlite3"
    store = Store(db_path)
    request = {"batch_id": "kill-me", "round": 0, "injection": {"type": "response_timeout"}}
    store.prepare_batch(
        batch_id="kill-me",
        request_sha256=sha256_json(request),
        round_no=0,
        request_payload=request,
    )
    with pytest.raises(AppError) as raised:
        store.get_final_batch("kill-me")
    assert raised.value.code is ErrorCode.BATCH_NOT_READY

    restarted = Store(db_path)
    reports = restarted.recover_incomplete()
    assert len(reports) == 1
    report = reports[0]
    assert report["before"]["state"] == "INJECTING"
    assert report["after"]["state"] == "ABORTED"
    assert report["diff"]["exit_report_complete"] is False
    assert report["before"]["committed_fault_events"] == 0

    batch = restarted.get_final_batch("kill-me")
    assert batch["state"] == "ABORTED"
    assert batch["final_code"] == ErrorCode.RECOVERED_AFTER_RESTART.value
    assert batch["aggregation"]["status"] == "ABORTED"
    assert batch["recovery"]["action"] == "MARK_ABORTED_FROM_COMMITTED_INTENT"
    assert restarted.recover_incomplete() == []


def test_invalid_commit_transition_rolls_back_all_rows(tmp_path) -> None:
    store = Store(tmp_path / "rollback.sqlite3")
    request = {"batch_id": "bad-chain"}
    store.prepare_batch(
        batch_id="bad-chain",
        request_sha256=sha256_json(request),
        round_no=0,
        request_payload=request,
    )
    now = datetime.now(UTC).isoformat()
    illegal_chain = [
        {
            "sequence": 1,
            "from_state": "INJECTING",
            "to_state": "DEGRADED",
            "reason": "first",
            "code": "E_FAULT_CONTENT_TRUNCATED",
            "at": now,
        },
        {
            "sequence": 2,
            "from_state": "DEGRADED",
            "to_state": "RETRYING",
            "reason": "illegal",
            "code": "E_INVALID_TRANSITION",
            "at": now,
        },
    ]
    with pytest.raises(AppError) as raised:
        store.commit_final(
            batch_id="bad-chain",
            final_state=BatchState.VERIFIED,
            final_code=ErrorCode.OK,
            transitions=illegal_chain,
            fault_events=[],
            retry_attempts=[],
            aggregation=_aggregation(),
        )
    assert raised.value.code is ErrorCode.INVALID_TRANSITION
    with pytest.raises(AppError) as query_error:
        store.get_final_batch("bad-chain")
    assert query_error.value.code is ErrorCode.BATCH_NOT_READY
    assert store.count_faults("bad-chain") == 0


def test_same_batch_id_with_different_payload_is_rejected(tmp_path) -> None:
    store = Store(tmp_path / "idempotency.sqlite3")
    store.prepare_batch(
        batch_id="same-id",
        request_sha256=sha256_json({"round": 0}),
        round_no=0,
        request_payload={"round": 0},
    )
    with pytest.raises(AppError) as raised:
        store.prepare_batch(
            batch_id="same-id",
            request_sha256=sha256_json({"round": 1}),
            round_no=1,
            request_payload={"round": 1},
        )
    assert raised.value.code is ErrorCode.BATCH_PAYLOAD_CONFLICT


def test_handled_runtime_error_aborts_intent_instead_of_leaving_it_running(tmp_path) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    dead_port = int(sock.getsockname()[1])
    sock.close()
    config = replace(
        LabConfig.load("config/default.json"),
        injector_port=dead_port,
        database_path=tmp_path / "runtime-error.sqlite3",
    )
    store = Store(config.database_path)
    orchestrator = BatchOrchestrator(config, store)
    record = orchestrator.execute(
        BatchRequest(
            batch_id="runtime-injector-down",
            round=0,
            injection={
                "type": "response_timeout",
                "target_node": "node-1",
                "target_shard": 0,
                "round": 0,
            },
        )
    )
    assert record["state"] == "ABORTED"
    assert record["final_code"] == ErrorCode.INJECTOR_UNAVAILABLE.value
    assert record["is_final"] is True
    assert record["transitions"][-1]["to_state"] == "ABORTED"


def test_final_batch_cannot_transition_again(tmp_path) -> None:
    store = Store(tmp_path / "final-transition.sqlite3")
    payload = {"batch_id": "already-final"}
    store.prepare_batch(
        batch_id="already-final",
        request_sha256=sha256_json(payload),
        round_no=0,
        request_payload=payload,
    )
    now = datetime.now(UTC).isoformat()
    verified_transition = [
        {
            "sequence": 1,
            "from_state": "INJECTING",
            "to_state": "VERIFIED",
            "reason": "verified",
            "code": "E_OK",
            "at": now,
        }
    ]
    store.commit_final(
        batch_id="already-final",
        final_state=BatchState.VERIFIED,
        final_code=ErrorCode.OK,
        transitions=verified_transition,
        fault_events=[],
        retry_attempts=[],
        aggregation={
            **_aggregation(),
            "status": "COMPLETE",
            "participant_nodes": ["node-0"],
            "participant_blocks": 1,
            "tensor": [0.0],
            "checksum": "probe",
        },
    )
    with pytest.raises(AppError) as raised:
        store.commit_final(
            batch_id="already-final",
            final_state=BatchState.ABORTED,
            final_code=ErrorCode.AGGREGATION_ABORTED,
            transitions=[],
            fault_events=[],
            retry_attempts=[],
            aggregation=_aggregation(),
        )
    assert raised.value.code is ErrorCode.INVALID_TRANSITION
    assert store.get_final_batch("already-final")["state"] == "VERIFIED"

