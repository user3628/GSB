import time

from fiw.faults import (
    execute_corrupt_body,
    execute_refused,
    execute_timeout,
)
from fiw.models import (
    OUTCOME_INJECTED,
    OUTCOME_TIMEOUT,
    OUTCOME_UNSUPPORTED,
)

TIMEOUT_MS = 250


def test_refused_on_unlistened_port(env):
    verdict = execute_refused("127.0.0.1", env.unlistened_port, TIMEOUT_MS)
    assert verdict.outcome == OUTCOME_INJECTED
    assert verdict.evidence["result"] == "ECONNREFUSED"


def test_refused_on_listening_port_unsupported(env):
    verdict = execute_refused("127.0.0.1", env.healthy_1.port, TIMEOUT_MS)
    assert verdict.outcome == OUTCOME_UNSUPPORTED
    assert verdict.code == "E_FAULT_UNSUPPORTED"
    assert "listening" in verdict.reason


def test_timeout_on_blackhole(env):
    started = time.monotonic()
    verdict = execute_timeout("127.0.0.1", env.blackhole.port, TIMEOUT_MS)
    elapsed_ms = (time.monotonic() - started) * 1000
    assert verdict.outcome == OUTCOME_TIMEOUT
    assert verdict.code == "E_INJECTION_TIMEOUT"
    assert verdict.evidence["socket_closed"] is True
    assert elapsed_ms >= TIMEOUT_MS * 0.8  # real wait, not an instant fake


def test_timeout_releases_socket_resources(env):
    for _ in range(25):
        verdict = execute_timeout("127.0.0.1", env.blackhole.port, 60)
        assert verdict.outcome == OUTCOME_TIMEOUT


def test_timeout_on_responding_endpoint_unsupported(env):
    verdict = execute_timeout("127.0.0.1", env.healthy_1.port, TIMEOUT_MS)
    assert verdict.outcome == OUTCOME_UNSUPPORTED
    assert "responded" in verdict.reason


def test_timeout_on_unlistened_port_unsupported(env):
    verdict = execute_timeout("127.0.0.1", env.unlistened_port, TIMEOUT_MS)
    assert verdict.outcome == OUTCOME_UNSUPPORTED
    assert "not listening" in verdict.reason


def test_corrupt_body_on_healthy_endpoint(env):
    verdict = execute_corrupt_body("127.0.0.1", env.healthy_1.port, TIMEOUT_MS)
    assert verdict.outcome == OUTCOME_INJECTED
    assert "parse_error" in verdict.evidence


def test_corrupt_body_on_blackhole_unsupported(env):
    verdict = execute_corrupt_body("127.0.0.1", env.blackhole.port, TIMEOUT_MS)
    assert verdict.outcome == OUTCOME_UNSUPPORTED
    assert verdict.code == "E_FAULT_UNSUPPORTED"


def test_corrupt_body_on_unlistened_unsupported(env):
    verdict = execute_corrupt_body("127.0.0.1", env.unlistened_port, TIMEOUT_MS)
    assert verdict.outcome == OUTCOME_UNSUPPORTED
