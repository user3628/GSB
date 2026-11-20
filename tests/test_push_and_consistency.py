from __future__ import annotations

import asyncio
import socket
import threading
import time

import pytest

from confighub.errors import ConfigHubError, ErrorCode
from confighub.models import PushState, SampleSpec
from confighub.pipeline import compute_pipeline

from .conftest import CONFIG_KEYS, SAMPLE_SEED, SAMPLE_SIZE, register, submit


def test_push_timeout_really_closes_underlying_connection(service):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = int(listener.getsockname()[1])
    eof_seen = threading.Event()
    errors: list[str] = []

    def never_respond() -> None:
        try:
            connection, _ = listener.accept()
            connection.settimeout(5)
            while True:
                chunk = connection.recv(65536)
                if not chunk:
                    eof_seen.set()
                    break
            connection.close()
        except Exception as exc:  # pragma: no cover - diagnostic path
            errors.append(repr(exc))
        finally:
            listener.close()

    thread = threading.Thread(target=never_respond, daemon=True)
    thread.start()
    service.register_client(
        "timeout-client",
        CONFIG_KEYS,
        f"http://127.0.0.1:{port}/never",
        timeout_seconds=0.12,
        sample_seed=SAMPLE_SEED,
        sample_size=SAMPLE_SIZE,
    )
    submit(service, 1, {
        "w0": 1.0,
        "w1": 1.0,
        "w2": 1.0,
        "normalization_scale": 1.0,
        "threshold": 0.0,
    })
    started = time.monotonic()
    report = asyncio.run(service.push_version(1))
    elapsed = time.monotonic() - started
    assert elapsed < 1.0
    assert report["not_advanced"][0]["code"] == ErrorCode.E_PUSH_TIMEOUT
    assert report["not_advanced"][0]["connection_closed"] is True
    assert eof_seen.wait(2.0), errors
    row = service.storage.query_one(
        "SELECT state, last_error FROM push_records WHERE client_id='timeout-client'"
    )
    assert row == {"state": "ABORTED", "last_error": ErrorCode.E_PUSH_TIMEOUT}
    thread.join(timeout=2)


def test_stale_picked_version_is_excluded_from_final_conclusion(
    service, endpoint, config_v1, config_v2
):
    endpoint.set_mode("lagging", "lagging")
    register(service, endpoint, "fast", "fast")
    register(service, endpoint, "lagging", "lagging")
    submit(service, 1, config_v1)
    first = asyncio.run(service.push_version(1))
    assert {row["client_id"] for row in first["advanced"]} == {"fast", "lagging"}

    submit(service, 2, config_v2)
    second = asyncio.run(service.push_version(2))
    assert [row["client_id"] for row in second["advanced"]] == ["fast"]
    stale = next(row for row in second["not_advanced"] if row["client_id"] == "lagging")
    assert stale["code"] == ErrorCode.E_STALE_ACK
    assert service.get_client("lagging")["applied_version"] == 1
    assert service.get_client("fast")["applied_version"] == 2

    consistency = second["consistency"]
    assert consistency["conclusion"] == "CONSISTENT"
    assert consistency["consistent"] is False
    assert consistency["final_consistent"] is True
    assert consistency["observed_conclusion"] == "INCONSISTENT"
    assert consistency["observed_consistent"] is False
    assert consistency["mixed_versions"] is True
    assert consistency["observed_versions"] == [1, 2]
    assert consistency["coverage_complete"] is False
    assert consistency["eligible_clients"] == ["fast"]
    assert consistency["final_version"] == 2
    detail = consistency["inconsistent_details"][0]
    assert detail["client_id"] == "lagging"
    assert detail["picked_version"] == 1
    assert detail["eligible_for_final"] is False
    assert "result_digest" in detail["difference_fields"]
    assert "stats.normalized.mean" in detail["difference_fields"]
    assert consistency["excluded_results"][0]["picked_version"] == 1


def test_duplicate_conflicting_ack_and_late_ack(service, endpoint, config_v1, config_v2):
    register(service, endpoint, "client", "client")
    submit(service, 1, config_v1)
    asyncio.run(service.push_version(1))
    result_v1 = compute_pipeline(
        SampleSpec(SAMPLE_SEED, SAMPLE_SIZE, 3),
        1,
        config_v1,
    )
    duplicate = service.apply_ack("client", 1, result_v1)
    assert duplicate["duplicate"] is True
    assert duplicate["state"] == PushState.APPLIED.value

    with pytest.raises(ConfigHubError) as conflict:
        service.apply_ack("client", 1, {**result_v1, "result_digest": "0" * 64})
    assert conflict.value.code == ErrorCode.E_CONFLICTING_ACK
    assert service.get_client("client")["applied_version"] == 1
    result_count = service.storage.query_one(
        "SELECT count(*) AS n FROM results WHERE client_id='client' AND version=1"
    )["n"]
    assert result_count == 1

    submit(service, 2, config_v2)
    asyncio.run(service.push_version(2))
    assert service.get_client("client")["applied_version"] == 2

    # A newer snapshot supersedes v2 before its (late) external confirmation.
    submit(service, 3, {**config_v2, "threshold": 0.9})
    result_v2 = compute_pipeline(
        SampleSpec(SAMPLE_SEED, SAMPLE_SIZE, 3),
        2,
        config_v2,
    )
    with pytest.raises(ConfigHubError) as stale:
        service.apply_ack("client", 2, result_v2)
    assert stale.value.code == ErrorCode.E_STALE_ACK
    assert service.get_client("client")["applied_version"] == 2
    stale_history = service.storage.query_one(
        """
        SELECT accepted, code FROM ack_records
        WHERE client_id='client' AND version=2
        ORDER BY id DESC LIMIT 1
        """
    )
    assert stale_history == {"accepted": 0, "code": ErrorCode.E_STALE_ACK}


def test_duplicate_push_conflict_and_concurrent_single_execution(service, endpoint, config_v1):
    register(service, endpoint, "client", "client")
    submit(service, 1, config_v1)

    async def run_twice():
        return await asyncio.gather(
            service.push_version(1),
            service.push_version(1),
        )

    first, second = asyncio.run(run_twice())
    assert first["batch_id"] == second["batch_id"]
    assert sorted([first["reused"], second["reused"]]) == [False, True]
    assert endpoint.stats()["client"]["request_count"] == 1

    with pytest.raises(ConfigHubError) as conflict:
        asyncio.run(service.push_version(1, content_tag="different"))
    assert conflict.value.code == ErrorCode.E_CONFLICTING_PUSH
    assert endpoint.stats()["client"]["request_count"] == 1


def test_illegal_transition_rolls_back_domain_state(service, endpoint, config_v1):
    register(service, endpoint, "client", "client")
    submit(service, 1, config_v1)
    asyncio.run(service.push_version(1))
    before = service.storage.query_one(
        "SELECT state FROM push_records WHERE client_id='client' AND version=1"
    )["state"]
    with pytest.raises(ConfigHubError) as invalid:
        service.transition_push_state("client", 1, "PENDING", "illegal-test")
    after = service.storage.query_one(
        "SELECT state FROM push_records WHERE client_id='client' AND version=1"
    )["state"]
    assert invalid.value.code == ErrorCode.E_INVALID_TRANSITION
    assert before == after == PushState.APPLIED.value
    assert service.get_client("client")["applied_version"] == 1


def test_reader_never_sees_acked_with_old_client_version(service, endpoint, config_v1):
    register(service, endpoint, "client", "client")
    submit(service, 1, config_v1)
    entered = threading.Event()
    release = threading.Event()
    original = service._transition

    def paused_transition(connection, client_id, version, target, reason):
        result = original(connection, client_id, version, target, reason)
        if target is PushState.ACKED:
            entered.set()
            if not release.wait(5):
                raise RuntimeError("test did not release acknowledgement transaction")
        return result

    service._transition = paused_transition
    failure: list[BaseException] = []

    def execute_push() -> None:
        try:
            asyncio.run(service.push_version(1))
        except BaseException as exc:  # pragma: no cover - surfaced below
            failure.append(exc)

    thread = threading.Thread(target=execute_push, daemon=True)
    thread.start()
    try:
        assert entered.wait(5), "ack transaction did not reach ACKED"
        visible_push = service.storage.query_one(
            "SELECT state FROM push_records WHERE client_id='client' AND version=1"
        )
        visible_client = service.get_client("client")
        assert visible_push["state"] == PushState.PUSHED.value
        assert visible_client["applied_version"] is None
        assert service.storage.visibility_invariant_violations() == []
    finally:
        release.set()
        thread.join(timeout=10)
    assert not failure
    assert service.storage.query_one(
        "SELECT state FROM push_records WHERE client_id='client' AND version=1"
    )["state"] == PushState.APPLIED.value
    assert service.get_client("client")["applied_version"] == 1
    transitions = service.storage.query_all(
        "SELECT from_state, to_state FROM push_transitions ORDER BY id"
    )
    assert {"from_state": "PUSHED", "to_state": "ACKED"} in transitions
    assert {"from_state": "ACKED", "to_state": "APPLIED"} in transitions

