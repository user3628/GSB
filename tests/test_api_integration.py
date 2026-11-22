from __future__ import annotations

import concurrent.futures
import os
import time
from typing import Any

import httpx

from grad_fault_lab.errors import ErrorCode
from grad_fault_lab.injection import perform_injection, validate_injection_against_config
from grad_fault_lab.models import InjectionSpec


def _post_batch(lab: Any, batch_id: str, round_no: int, injection: dict[str, Any]):
    return lab.client.post(
        f"{lab.base_url}/v1/batches",
        json={"batch_id": batch_id, "round": round_no, "injection": injection},
    )


def test_five_real_injection_modes_and_classification(lab, batch_spec) -> None:
    expectations = {
        "fault-refused": (
            batch_spec("connection_refused", max_retries=1),
            200,
            ErrorCode.OK,
            ErrorCode.FAULT_CONNECTION_REFUSED,
            "RETRYABLE",
        ),
        "fault-timeout": (
            batch_spec("response_timeout", max_retries=1),
            200,
            ErrorCode.OK,
            ErrorCode.FAULT_RESPONSE_TIMEOUT,
            "RETRYABLE",
        ),
        "fault-truncated": (
            batch_spec("content_truncated", round_no=1),
            502,
            ErrorCode.FAULT_CONTENT_TRUNCATED,
            ErrorCode.FAULT_CONTENT_TRUNCATED,
            "FATAL",
        ),
        "fault-duplicate": (
            batch_spec("duplicate_submission", round_no=2),
            409,
            ErrorCode.FAULT_DUPLICATE_SUBMISSION,
            ErrorCode.FAULT_DUPLICATE_SUBMISSION,
            "FATAL",
        ),
        "fault-order": (
            batch_spec("order_mismatch", round_no=2, node_id="node-2"),
            409,
            ErrorCode.FAULT_ORDER_MISMATCH,
            ErrorCode.FAULT_ORDER_MISMATCH,
            "FATAL",
        ),
    }
    for batch_id, (spec, status, final_code, fault_code, classification) in expectations.items():
        response = _post_batch(lab, batch_id, spec["round"], spec)
        assert response.status_code == status, response.text
        query = lab.client.get(f"{lab.base_url}/v1/batches/{batch_id}")
        assert query.status_code == 200
        batch = query.json()["data"]
        assert batch["final_code"] == final_code.value
        assert batch["faults"][0]["code"] == fault_code.value
        assert batch["faults"][0]["classification"] == classification

    refused = lab.client.get(f"{lab.base_url}/v1/batches/fault-refused").json()["data"]
    refused_details = refused["faults"][0]["observation"]["details"]
    assert refused_details["operation"] == "tcp_connect"
    assert refused_details["socket_closed"] is True

    timeout = lab.client.get(f"{lab.base_url}/v1/batches/fault-timeout").json()["data"]
    timeout_details = timeout["faults"][0]["observation"]["details"]
    assert timeout_details["socket_closed"] is True
    assert timeout_details["timeout_seconds"] == 0.08
    assert len(timeout["retry_sequence"]) == 2

    truncated = lab.client.get(f"{lab.base_url}/v1/batches/fault-truncated").json()["data"]
    truncated_details = truncated["faults"][0]["observation"]["details"]
    assert truncated_details["received_bytes"] < truncated_details["declared_content_length"]
    assert len(truncated["retry_sequence"]) == 1

    duplicate = lab.client.get(f"{lab.base_url}/v1/batches/fault-duplicate").json()["data"]
    assert duplicate["faults"][0]["observation"]["details"]["aggregator_detection"] == (
        ErrorCode.FAULT_DUPLICATE_SUBMISSION.value
    )

    order = lab.client.get(f"{lab.base_url}/v1/batches/fault-order").json()["data"]
    assert order["faults"][0]["observation"]["details"]["aggregator_detection"] == (
        ErrorCode.FAULT_ORDER_MISMATCH.value
    )
    for batch in (truncated, duplicate, order):
        assert len(batch["retry_sequence"]) == 1


def test_timeout_closes_socket_descriptors(lab, batch_spec) -> None:
    spec = InjectionSpec.from_payload(batch_spec("response_timeout", timeout_seconds=0.02))
    validate_injection_against_config(lab.config, spec, 0)
    before = len(os.listdir("/proc/self/fd"))
    for _ in range(12):
        observation = perform_injection(
            lab.config, spec, attempt_no=1, block_count=lab.config.shards_per_node
        )
        assert observation.code is ErrorCode.FAULT_RESPONSE_TIMEOUT
        assert observation.details["socket_closed"] is True
    time.sleep(0.15)
    after = len(os.listdir("/proc/self/fd"))
    assert after <= before + 2


def test_shortened_set_and_error_bound_are_bound_to_faults(lab, batch_spec) -> None:
    spec = batch_spec(
        "response_timeout", round_no=3, max_retries=1, apply_attempts=[1, 2]
    )
    response = _post_batch(lab, "fault-shortened", 3, spec)
    assert response.status_code == 200, response.text
    batch = response.json()["data"]
    assert batch["state"] == "VERIFIED"
    assert batch["final_code"] == ErrorCode.DEGRADED_AGGREGATION.value
    assert [attempt["code"] for attempt in batch["retry_sequence"]] == [
        ErrorCode.FAULT_RESPONSE_TIMEOUT.value,
        ErrorCode.FAULT_RESPONSE_TIMEOUT.value,
    ]
    aggregation = batch["aggregation"]
    assert aggregation["degraded"] is True
    assert aggregation["participant_nodes"] == ["node-0", "node-2"]
    assert aggregation["omitted_nodes"] == ["node-1"]
    bounds = aggregation["error_bounds"]
    assert bounds["observed_deviation_l2"] <= bounds["upper_bound_l2"] + 1e-14
    assert bounds["within_bound"] is True
    assert aggregation["fault_ids"] == [fault["fault_id"] for fault in batch["faults"]]
    assert len(aggregation["fault_ids"]) == 2


def test_retry_exhaustion_with_abort_policy_returns_retry_exhausted(lab, batch_spec) -> None:
    spec = batch_spec(
        "connection_refused", round_no=4, max_retries=1, apply_attempts=[1, 2]
    )
    response = _post_batch(lab, "retry-exhausted-abort", 4, spec)
    assert response.status_code == 503, response.text
    assert response.json()["error"]["code"] == ErrorCode.RETRY_EXHAUSTED.value
    batch = lab.client.get(f"{lab.base_url}/v1/batches/retry-exhausted-abort").json()["data"]
    assert batch["state"] == "ABORTED"
    assert len(batch["retry_sequence"]) == 2
    assert batch["transitions"][-1]["to_state"] == "ABORTED"


def test_fatal_fault_can_use_shorten_policy_without_retrying(lab, batch_spec) -> None:
    spec = batch_spec("duplicate_submission", round_no=0, max_retries=2)
    response = _post_batch(lab, "fatal-shorten", 0, spec)
    assert response.status_code == 200, response.text
    batch = response.json()["data"]
    assert batch["state"] == "VERIFIED"
    assert batch["final_code"] == ErrorCode.DEGRADED_AGGREGATION.value
    assert len(batch["retry_sequence"]) == 1
    assert batch["aggregation"]["degraded"] is True
    assert batch["aggregation"]["omitted_nodes"] == ["node-1"]


def test_invalid_spec_does_not_create_intent(lab, batch_spec) -> None:
    before = lab.store.count_batches()
    bad = batch_spec("response_timeout")
    bad["target_node"] = "node-404"
    response = _post_batch(lab, "invalid-node-batch", 0, bad)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == ErrorCode.INJECTION_SPEC_INVALID.value
    assert lab.store.count_batches() == before

    response = _post_batch(lab, "invalid-shape-batch", 0, "not-an-object")
    assert response.status_code == 422
    assert response.json()["error"]["code"] == ErrorCode.INJECTION_SPEC_INVALID.value
    assert lab.store.count_batches() == before

    bad_type = batch_spec("not_a_fault")
    response = _post_batch(lab, "invalid-type-batch", 0, bad_type)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == ErrorCode.INJECTION_SPEC_INVALID.value
    assert lab.store.count_batches() == before


def test_idempotent_triggers_and_concurrent_readers(lab, batch_spec) -> None:
    spec = batch_spec("response_timeout", max_retries=1)
    payload = {"batch_id": "concurrent-idempotent", "round": 0, "injection": spec}

    def submit() -> tuple[int, dict[str, Any]]:
        with httpx.Client(trust_env=False, timeout=30.0) as client:
            response = client.post(f"{lab.base_url}/v1/batches", json=payload)
            return response.status_code, response.json()

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda _: submit(), range(6)))
    assert {status for status, _ in results} == {200}
    updated_times = {body["data"]["updated_at"] for _, body in results}
    assert len(updated_times) == 1
    assert lab.store.count_faults("concurrent-idempotent") == 1

    def read() -> tuple[int, str, str]:
        response = lab.client.get(f"{lab.base_url}/v1/batches/concurrent-idempotent")
        data = response.json()["data"]
        return response.status_code, data["state"], data["aggregation"]["checksum"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        reads = list(pool.map(lambda _: read(), range(24)))
    assert len(reads) == 24
    assert len({checksum for _, _, checksum in reads}) == 1
    assert all(status == 200 and state == "VERIFIED" for status, state, _ in reads)


def test_failed_batch_replay_reuses_committed_failure(lab, batch_spec) -> None:
    spec = batch_spec("content_truncated", round_no=1)
    first = _post_batch(lab, "idempotent-failure", 1, spec)
    second = _post_batch(lab, "idempotent-failure", 1, spec)
    assert first.status_code == second.status_code == 502
    assert first.json() == second.json()
    first_batch = lab.client.get(f"{lab.base_url}/v1/batches/idempotent-failure").json()["data"]
    assert lab.store.count_faults("idempotent-failure") == 1
    assert len(first_batch["retry_sequence"]) == 1


def test_queries_cannot_observe_intent_without_final_aggregation(lab) -> None:
    batch_id = "intent-only-visible-test"
    request = {"batch_id": batch_id, "round": 0, "injection": {"type": "response_timeout"}}
    from grad_fault_lab.models import sha256_json

    lab.store.prepare_batch(
        batch_id=batch_id,
        request_sha256=sha256_json(request),
        round_no=0,
        request_payload=request,
    )
    response = lab.client.get(f"{lab.base_url}/v1/batches/{batch_id}")
    assert response.status_code == 409
    body = response.json()
    assert body["error"]["code"] == ErrorCode.BATCH_NOT_READY.value
    assert "faults" not in body["error"]["details"]
    reports = lab.store.recover_incomplete()
    assert any(report["batch_id"] == batch_id for report in reports)


def test_request_validation_uses_unified_error_body(lab) -> None:
    response = lab.client.post(
        f"{lab.base_url}/v1/batches",
        json={"batch_id": "x", "round": -1, "injection": {}},
    )
    assert response.status_code == 422
    body = response.json()
    assert body["ok"] is False
    assert body["error"]["code"] == ErrorCode.REQUEST_VALIDATION.value
    assert set(body["error"]) >= {"code", "message", "retryable", "details"}


def test_unknown_route_uses_unified_error_body(lab) -> None:
    response = lab.client.get(f"{lab.base_url}/v1/does-not-exist")
    assert response.status_code == 404
    body = response.json()
    assert body["ok"] is False
    assert body["error"]["code"] == ErrorCode.NOT_FOUND.value
    assert body["error"]["details"]["path"] == "/v1/does-not-exist"

