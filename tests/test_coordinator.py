from __future__ import annotations

import json
import random
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from leasecoord import BusinessError, Coordinator, CoordinatorConfig

BASE = 1_760_000_000_000


class ProtocolClock:
    def __init__(self, now_ms: int = BASE) -> None:
        self.value = now_ms

    def now_ms(self) -> int:
        return self.value


def make_config(**overrides: object) -> CoordinatorConfig:
    defaults: dict[str, object] = {
        "exact_limit": 8,
        "heartbeat_timeout_ms": 1000,
        "min_lease_ttl_ms": 1,
        "max_lease_ttl_ms": 60_000,
    }
    defaults.update(overrides)
    return CoordinatorConfig(**defaults)


def make_coordinator(
    tmp_path: Path,
    *,
    name: str = "coord.sqlite",
    now_ms: int = BASE,
    auto_recover: bool = False,
    **config_overrides: object,
) -> Coordinator:
    return Coordinator(
        tmp_path / name,
        make_config(**config_overrides),
        clock=ProtocolClock(now_ms),
        auto_recover=auto_recover,
    )


def register(co: Coordinator, node_id: str, priority: int, now: int, ttl: int = 5000) -> dict:
    return co.register_node(
        node_id,
        priority,
        ttl,
        last_heartbeat_ms=now,
        now_ms=now,
    )


def expect_error(code: str, fn, *args, **kwargs) -> BusinessError:
    with pytest.raises(BusinessError) as exc_info:
        fn(*args, **kwargs)
    assert exc_info.value.code == code
    return exc_info.value


def test_registration_validation_tie_break_and_lease_continuity(tmp_path: Path) -> None:
    co = make_coordinator(tmp_path)
    register(co, "bravo", 90, BASE)
    register(co, "alpha", 90, BASE)

    first = register(co, "charlie", 10, BASE)
    assert first["lease_version"] == 0

    campaign = co.campaign("bravo", expected_lease_version=0, now_ms=BASE)
    assert campaign["leader"]["id"] == "alpha"
    assert campaign["lease_version"] == 1
    assert campaign["election"]["algorithm"] == "exact_full_scan"
    assert campaign["election"]["estimated"] is False
    assert campaign["election"]["winner_evidence"]["tie_break"].startswith(
        "lexicographically"
    )

    before = co.get_lease(now_ms=BASE)
    invalid = expect_error(
        "E_NODE_INVALID", co.campaign, "missing-node", now_ms=BASE
    )
    assert invalid.context["reason"] == "node_not_registered"
    after = co.get_lease(now_ms=BASE)
    assert after["lease_version"] == before["lease_version"]
    assert after["lease"]["state"] == "GRANTED"

    expect_error(
        "E_NODE_INVALID",
        co.register_node,
        "bad-priority",
        999,
        5000,
        now_ms=BASE,
    )

    updated = co.register_node(
        "alpha",
        91,
        7000,
        last_heartbeat_ms=BASE,
        now_ms=BASE + 10,
    )
    assert updated["operation"] == "updated"
    assert updated["registered_version"] == 2
    assert updated["lease_version"] == 1
    assert updated["node"]["status"] == "LEADER"


def test_stale_campaign_is_invalid_and_does_not_change_lease(tmp_path: Path) -> None:
    co = make_coordinator(tmp_path)
    register(co, "stale", 100, BASE)
    before = co.get_lease(now_ms=BASE)
    error = expect_error(
        "E_NODE_INVALID", co.campaign, "stale", now_ms=BASE + 1001
    )
    assert error.context["reason"] == "heartbeat_timeout"
    after = co.get_lease(now_ms=BASE)
    assert after["lease_version"] == before["lease_version"] == 0
    assert after["lease"]["state"] == "FREE"


def test_renew_resign_and_next_election(tmp_path: Path) -> None:
    co = make_coordinator(tmp_path, exact_limit=8)
    register(co, "alpha", 100, BASE, ttl=10_000)
    register(co, "bravo", 80, BASE, ttl=10_000)
    granted = co.campaign("alpha", now_ms=BASE)
    assert granted["lease_version"] == 1
    first_expiry = granted["lease"]["expires_at_ms"]

    renewed = co.renew("alpha", expected_lease_version=1, now_ms=BASE + 500)
    assert renewed["lease_version"] == 2
    assert renewed["lease"]["expires_at_ms"] > first_expiry
    assert renewed["leader"]["leader_lease_version"] == 2

    expect_error(
        "E_STALE_LEASE",
        co.renew,
        "alpha",
        expected_lease_version=1,
        now_ms=BASE + 600,
    )
    assert co.get_lease(now_ms=BASE + 600)["lease_version"] == 2

    resigned = co.resign("alpha", expected_lease_version=2, now_ms=BASE + 700)
    assert resigned["lease_version"] == 3
    assert resigned["lease"]["state"] == "FREE"
    assert co.get_leader(now_ms=BASE + 700)["has_leader"] is False

    register(co, "bravo", 80, BASE + 1500, ttl=10_000)
    next_election = co.campaign("bravo", expected_lease_version=3, now_ms=BASE + 1500)
    assert next_election["lease_version"] == 4
    assert next_election["leader"]["id"] == "bravo"
    assert next_election["leader"]["status"] == "LEADER"
    assert next_election["term"] == 2


def test_failover_no_candidate_preserves_version_then_succeeds(tmp_path: Path) -> None:
    co = make_coordinator(tmp_path, heartbeat_timeout_ms=1000)
    register(co, "alpha", 100, BASE, ttl=5000)
    register(co, "bravo", 80, BASE, ttl=5000)
    assert co.campaign("alpha", now_ms=BASE)["lease_version"] == 1

    failed_at = BASE + 1500
    error = expect_error(
        "E_NO_ELIGIBLE_CANDIDATE",
        co.failover,
        expected_lease_version=1,
        now_ms=failed_at,
    )
    assert error.context["preserved_lease_version"] == 1
    assert error.context["candidate_set_digest"]
    assert error.context["trigger_reason"] == "leader_heartbeat_timeout"
    lease_after_failure = co.get_lease(now_ms=failed_at)
    assert lease_after_failure["lease_version"] == 1
    assert lease_after_failure["lease"]["stored_state"] == "GRANTED"
    assert lease_after_failure["lease"]["state"] == "EXPIRED"
    assert lease_after_failure["lease"]["holder_status"] == "SUSPECT"
    assert co.get_leader(now_ms=failed_at)["has_leader"] is False

    co.heartbeat("bravo", heartbeat_ms=failed_at, now_ms=failed_at)
    failover = co.failover(expected_lease_version=1, now_ms=failed_at)
    assert failover["lease_version"] == 2
    assert failover["term"] == 2
    assert failover["leader"]["id"] == "bravo"
    assert failover["failover"]["trigger_reason"] == "leader_heartbeat_timeout"
    assert failover["failover"]["candidate_set_digest"]
    assert failover["failover"]["failed_holder_id"] == "alpha"
    old_alpha = co.get_node("alpha", now_ms=failed_at)
    assert old_alpha["stored_status"] == "SUSPECT"


def _adaptive_population(count: int, now_ms: int) -> list[tuple[str, int]]:
    return [
        (f"n-{index:04d}", 100 - (index % 20))
        for index in range(count)
    ]


def test_exact_and_sampled_paths_choose_same_leader(tmp_path: Path) -> None:
    population = _adaptive_population(240, BASE)
    exact = make_coordinator(tmp_path, name="exact.sqlite", exact_limit=300)
    sampled = make_coordinator(tmp_path, name="sampled.sqlite", exact_limit=10)
    for node_id, priority in population:
        register(exact, node_id, priority, BASE)
        register(sampled, node_id, priority, BASE)

    exact_result = exact.campaign("n-0239", now_ms=BASE)
    sampled_result = sampled.campaign("n-0239", now_ms=BASE)
    assert exact_result["leader"]["id"] == sampled_result["leader"]["id"] == "n-0000"
    exact_basis = exact_result["election"]
    sampled_basis = sampled_result["election"]
    assert exact_basis["algorithm"] == "exact_full_scan"
    assert exact_basis["estimated"] is False
    assert sampled_basis["algorithm"] == "priority_stratified_sampling"
    assert sampled_basis["estimated"] is True
    assert sampled_basis["estimated_winner"] is True
    assert sampled_basis["sampling_rate"] < 1.0
    assert sampled_basis["buckets"]
    assert sampled_basis["confidence"]["level"] == 0.95
    assert sampled_basis["difference_vs_exact"]["same_winner"] is True
    assert (
        exact_basis["candidate_set_digest"]
        == sampled_basis["candidate_set_digest"]
    )
    assert sampled_basis["winner_evidence"]["decisive_bucket_exhaustive"] is True


def test_same_node_concurrent_campaigns_have_one_writer(tmp_path: Path) -> None:
    co = make_coordinator(tmp_path)
    register(co, "alpha", 100, BASE)

    def attempt(_: int) -> tuple[str, dict]:
        try:
            return ("ok", co.campaign("alpha", expected_lease_version=0, now_ms=BASE))
        except BusinessError as exc:
            return ("error", exc.to_dict())

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(attempt, range(8)))
    successes = [item for item in results if item[0] == "ok"]
    failures = [item for item in results if item[0] == "error"]
    assert len(successes) == 1
    assert len(failures) == 7
    assert all(item[1]["code"] == "E_LEASE_HELD" for item in failures)
    assert all("holder_id" in item[1]["context"] for item in failures)
    assert co.get_lease(now_ms=BASE)["lease_version"] == 1


def test_cross_process_campaign_has_one_writer(tmp_path: Path) -> None:
    db_path = tmp_path / "processes.sqlite"
    config = make_config()
    setup = Coordinator(
        db_path,
        config,
        clock=ProtocolClock(BASE),
        auto_recover=False,
    )
    node_ids = [f"proc-{index}" for index in range(6)]
    for index, node_id in enumerate(node_ids):
        register(setup, node_id, 100 - index, BASE)

    start_at = time.time() + 1.2
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "tests.campaign_child",
                str(db_path),
                node_id,
                "--now-ms",
                str(BASE),
                "--start-at",
                str(start_at),
            ],
            cwd=Path(__file__).resolve().parents[1],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for node_id in node_ids
    ]
    payloads = []
    for process in processes:
        stdout, stderr = process.communicate(timeout=20)
        assert process.returncode == 0, stderr
        payloads.append(json.loads(stdout.strip().splitlines()[-1]))

    successes = [item for item in payloads if item["outcome"] == "ok"]
    failures = [item for item in payloads if item["outcome"] == "error"]
    assert len(successes) == 1
    assert len(failures) == 5
    assert {item["error"]["code"] for item in failures} == {"E_LEASE_HELD"}
    winner = successes[0]["result"]["leader"]["id"]
    assert all(item["error"]["context"]["holder_id"] == winner for item in failures)
    assert setup.get_lease(now_ms=BASE)["lease_version"] == 1


def test_heartbeat_ordering_and_suspect_recovery(tmp_path: Path) -> None:
    co = make_coordinator(tmp_path, heartbeat_timeout_ms=300)
    co.register_node(
        "alpha",
        50,
        5000,
        last_heartbeat_ms=BASE - 200,
        now_ms=BASE,
    )
    older = co.heartbeat("alpha", heartbeat_ms=BASE - 250, now_ms=BASE)
    assert older["node"]["last_heartbeat_ms"] == BASE - 200
    assert older["heartbeat_advanced"] is False
    newer = co.heartbeat("alpha", heartbeat_ms=BASE, now_ms=BASE)
    assert newer["node"]["last_heartbeat_ms"] == BASE
    assert newer["heartbeat_advanced"] is True
    expect_error("E_NODE_INVALID", co.heartbeat, "missing", now_ms=BASE)

    # Recover a persisted SUSPECT state through a fresh heartbeat.
    co.admin_transition("alpha", "SUSPECT", now_ms=BASE)
    recovered = co.heartbeat("alpha", heartbeat_ms=BASE + 100, now_ms=BASE + 100)
    assert recovered["node"]["stored_status"] == "ACTIVE"


def test_invalid_transition_does_not_mutate_state(tmp_path: Path) -> None:
    co = make_coordinator(tmp_path)
    register(co, "alpha", 50, BASE)
    before = co.get_lease(now_ms=BASE)
    error = expect_error(
        "E_INVALID_TRANSITION",
        co.admin_transition,
        "alpha",
        "LEADER",
        now_ms=BASE,
    )
    assert error.context["target_status"] == "LEADER"
    after = co.get_lease(now_ms=BASE)
    assert after["lease_version"] == before["lease_version"] == 0
    assert co.get_node("alpha", now_ms=BASE)["stored_status"] == "ACTIVE"

    co.admin_transition("alpha", "SUSPECT", now_ms=BASE)
    expect_error(
        "E_INVALID_TRANSITION",
        co.admin_transition,
        "alpha",
        "FOLLOWER",
        now_ms=BASE,
    )
    assert co.get_node("alpha", now_ms=BASE)["stored_status"] == "SUSPECT"

    # A stored leader whose lease has effectively expired still needs the
    # explicit resign/failover path; admin transitions must not produce a
    # 500-level invariant failure.
    co.heartbeat("alpha", heartbeat_ms=BASE, now_ms=BASE)
    co.admin_transition("alpha", "ACTIVE", now_ms=BASE)
    register(co, "bravo", 100, BASE, ttl=5000)
    co.campaign("bravo", now_ms=BASE)
    stale_at = BASE + 1500
    expect_error(
        "E_INVALID_TRANSITION",
        co.admin_transition,
        "bravo",
        "SUSPECT",
        now_ms=stale_at,
    )
    assert co.get_node("bravo", now_ms=stale_at)["stored_status"] == "LEADER"


def test_forced_termination_recovery_aborts_orphan_intent(tmp_path: Path) -> None:
    db_path = tmp_path / "crash.sqlite"
    config = make_config()
    setup = Coordinator(
        db_path,
        config,
        clock=ProtocolClock(BASE),
        auto_recover=False,
    )
    register(setup, "alpha", 100, BASE, ttl=5000)
    register(setup, "bravo", 80, BASE, ttl=5000)
    assert setup.campaign("alpha", now_ms=BASE)["lease_version"] == 1

    child = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.crash_child",
            str(db_path),
            "--now-ms",
            str(BASE + 200),
            "--node",
            "alpha",
            "--expected-version",
            "1",
        ],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert child.returncode == 91, child.stderr

    with sqlite3.connect(db_path) as conn:
        pending = conn.execute(
            "SELECT status, kind FROM intents ORDER BY created_at_ms DESC LIMIT 1"
        ).fetchone()
        assert pending == ("PENDING", "RENEW")
        assert conn.execute("SELECT version FROM lease WHERE singleton=1").fetchone()[0] == 1

    recovered = Coordinator(
        db_path,
        config,
        clock=ProtocolClock(BASE + 1500),
        auto_recover=True,
    )
    report = recovered.get_recovery_report()
    assert report["available"] is True
    assert report["pending_intents_aborted_count"] == 1
    assert report["last_complete_lease"]["version"] == 1
    assert report["last_complete_lease"]["state"] == "GRANTED"
    assert set(report["heartbeat_recheck"]["invalid_node_ids"]) == {"alpha", "bravo"}
    assert report["heartbeat_recheck"]["requires_failover"] is True
    assert report["heartbeat_recheck"]["stale_lease_holder_ids"] == ["alpha"]
    assert report["facts_source"] == "sqlite_committed_state_only"
    assert any("cannot produce" in item for item in report["limitations"])
    assert recovered.get_lease(now_ms=BASE + 1500)["lease_version"] == 1

    with sqlite3.connect(db_path) as conn:
        aborted = conn.execute(
            "SELECT status, note FROM intents ORDER BY created_at_ms DESC LIMIT 1"
        ).fetchone()
        assert aborted[0] == "ABORTED"
        assert "orphaned intent" in aborted[1]


def test_idempotent_request_replay_and_conflict(tmp_path: Path) -> None:
    co = make_coordinator(tmp_path)
    register(co, "alpha", 100, BASE)
    first = co.campaign(
        "alpha",
        expected_lease_version=0,
        now_ms=BASE,
        request_id="req-1",
    )
    assert first["lease_version"] == 1
    replay = co.campaign(
        "alpha",
        expected_lease_version=0,
        now_ms=BASE,
        request_id="req-1",
    )
    assert replay["replayed"] is True
    assert replay["intent_id"] == first["intent_id"]
    assert co.get_lease(now_ms=BASE)["lease_version"] == 1
    expect_error(
        "E_CONFLICTING_REQUEST",
        co.renew,
        "alpha",
        expected_lease_version=1,
        now_ms=BASE + 10,
        request_id="req-1",
    )


def test_interleaved_lifecycle_keeps_commit_invariants(tmp_path: Path) -> None:
    randomizer = random.Random(19)
    co = make_coordinator(
        tmp_path,
        exact_limit=5,
        heartbeat_timeout_ms=700,
        sample_fraction=0.2,
    )
    node_ids = [f"state-{index}" for index in range(10)]
    now = BASE
    for index, node_id in enumerate(node_ids):
        register(co, node_id, 100 - index * 5, now, ttl=2000)

    observed_codes: set[str] = set()
    for _ in range(160):
        now += randomizer.randint(0, 250)
        node_id = randomizer.choice(node_ids)
        operation = randomizer.choice(
            ["campaign", "renew", "resign", "failover", "heartbeat", "register", "admin"]
        )
        try:
            if operation == "campaign":
                co.campaign(node_id, now_ms=now)
            elif operation == "renew":
                co.renew(node_id, now_ms=now)
            elif operation == "resign":
                co.resign(node_id, now_ms=now)
            elif operation == "failover":
                co.failover(force=randomizer.random() < 0.2, now_ms=now)
            elif operation == "heartbeat":
                co.heartbeat(node_id, heartbeat_ms=now, now_ms=now)
            elif operation == "register":
                co.register_node(
                    node_id,
                    100 - randomizer.randint(1, 10) * 5,
                    randomizer.choice([1000, 1500, 2000]),
                    last_heartbeat_ms=now,
                    now_ms=now,
                )
            else:
                co.admin_transition(
                    node_id,
                    randomizer.choice(["ACTIVE", "SUSPECT", "FOLLOWER"]),
                    now_ms=now,
                )
        except BusinessError as exc:
            observed_codes.add(exc.code)

        lease = co.get_lease(now_ms=now)
        if lease["lease"]["state"] == "GRANTED":
            assert lease["lease"]["holder_status"] == "LEADER"
            assert lease["leader"] is not None
            assert lease["leader"]["status"] == "LEADER"
            assert lease["leader"]["leader_lease_version"] == lease["lease_version"]
        else:
            assert lease["leader"] is None
    assert observed_codes <= {
        "E_NODE_INVALID",
        "E_LEASE_HELD",
        "E_NO_ELIGIBLE_CANDIDATE",
        "E_STALE_LEASE",
        "E_INVALID_TRANSITION",
    }
