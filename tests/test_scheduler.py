from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from gradsync.domain import FinalizationPolicy
from gradsync.errors import ErrorCode, GradSyncError
from gradsync.samples import RoundSample, build_sample_manifest, manifest_to_rounds
from gradsync.scheduler import GradientScheduler
from gradsync.store import SQLiteStore


class FakeClock:
    def __init__(self, now: float = 1_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def samples() -> dict[str, RoundSample]:
    return manifest_to_rounds(build_sample_manifest())


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def scheduler(tmp_path: Path, clock: FakeClock) -> GradientScheduler:
    return GradientScheduler(
        SQLiteStore(tmp_path / "scheduler.db"),
        clock=clock,
        allow_process_crash=False,
    )


def create_round(
    scheduler: GradientScheduler,
    sample: RoundSample,
    *,
    policy: str | None = None,
    collect_seconds: float | None = None,
) -> None:
    scheduler.create_round(
        sample.create_request(policy=policy, collect_seconds=collect_seconds)
    )


def submit_nodes(
    scheduler: GradientScheduler,
    sample: RoundSample,
    nodes: tuple[str, ...],
) -> None:
    for node_id in nodes:
        for item in sample.submissions_for(node_id):
            scheduler.submit(
                round_id=sample.round_id,
                node_id=item["node_id"],
                shard_id=item["shard_id"],
                sample_count=item["sample_count"],
                gradient=item["gradient"],
            )


def test_complete_weighted_aggregation_is_ready_and_reproducible(
    scheduler: GradientScheduler, samples: dict[str, RoundSample]
) -> None:
    sample = samples["demo-complete"]
    create_round(scheduler, sample)
    submit_nodes(scheduler, sample, sample.worker_ids)
    status = scheduler.get_round_status(sample.round_id)
    assert status["state"] == "READY"

    result = scheduler.finalize(round_id=sample.round_id)
    assert result["ok"] is True
    assert result["code"] == "OK"
    assert result["degraded"] is False
    assert result["missing_nodes"] == []
    assert result["digest_sha256"] == sample.expected_digest()
    assert result["element_sum"].count(".") == 1
    assert len(result["element_sum"].split(".")[1]) == 8
    assert len(result["tensor_preview"]) == sample.shard_count

    repeated = scheduler.finalize(round_id=sample.round_id)
    assert repeated["digest_sha256"] == result["digest_sha256"]
    aggregate_events = [
        event
        for event in scheduler.get_events(sample.round_id)
        if event["event_type"] == "ROUND_AGGREGATED"
    ]
    assert len(aggregate_events) == 1


def test_duplicate_and_conflicting_submissions_preserve_first_value(
    scheduler: GradientScheduler, samples: dict[str, RoundSample]
) -> None:
    sample = samples["demo-idempotent-a"]
    create_round(scheduler, sample)
    item = sample.submissions_for("worker-a")[0]
    first = scheduler.submit(
        round_id=sample.round_id,
        node_id=item["node_id"],
        shard_id=item["shard_id"],
        sample_count=item["sample_count"],
        gradient=item["gradient"],
    )
    duplicate = scheduler.submit(
        round_id=sample.round_id,
        node_id=item["node_id"],
        shard_id=item["shard_id"],
        sample_count=item["sample_count"],
        gradient=item["gradient"],
    )
    assert first["outcome"] == "ACCEPTED"
    assert duplicate["outcome"] == "DUPLICATE"
    assert len(scheduler.store.list_submissions(sample.round_id)) == 1

    conflicting = list(item["gradient"])
    conflicting[0] += 0.5
    with pytest.raises(GradSyncError) as error:
        scheduler.submit(
            round_id=sample.round_id,
            node_id=item["node_id"],
            shard_id=item["shard_id"],
            sample_count=item["sample_count"],
            gradient=conflicting,
        )
    assert error.value.code == ErrorCode.E_CONFLICTING_REPORT
    persisted = scheduler.store.get_submission(
        sample.round_id, item["node_id"], item["shard_id"]
    )
    assert persisted is not None
    assert persisted["content_hash"] == first["content_hash"]

    for remaining in sample.all_submissions():
        if (
            remaining["node_id"] == item["node_id"]
            and remaining["shard_id"] == item["shard_id"]
        ):
            continue
        scheduler.submit(
            round_id=sample.round_id,
            node_id=remaining["node_id"],
            shard_id=remaining["shard_id"],
            sample_count=remaining["sample_count"],
            gradient=remaining["gradient"],
        )
    result = scheduler.finalize(round_id=sample.round_id)
    assert result["digest_sha256"] == sample.expected_digest()


def test_arrival_order_does_not_change_digest(
    scheduler: GradientScheduler, samples: dict[str, RoundSample]
) -> None:
    first = samples["demo-idempotent-a"]
    second = samples["demo-idempotent-b"]
    create_round(scheduler, first)
    create_round(scheduler, second)

    for item in first.all_submissions():
        scheduler.submit(
            round_id=first.round_id,
            node_id=item["node_id"],
            shard_id=item["shard_id"],
            sample_count=item["sample_count"],
            gradient=item["gradient"],
        )
    reversed_items = list(reversed(second.all_submissions()))
    for item in reversed_items:
        scheduler.submit(
            round_id=second.round_id,
            node_id=item["node_id"],
            shard_id=item["shard_id"],
            sample_count=item["sample_count"],
            gradient=item["gradient"],
        )

    result_a = scheduler.finalize(round_id=first.round_id)
    result_b = scheduler.finalize(round_id=second.round_id)
    assert result_a["digest_sha256"] == result_b["digest_sha256"]
    assert result_a["tensor_preview"] == result_b["tensor_preview"]


def test_strict_mode_reserves_late_submission_and_reports_busy_before_window_ends(
    scheduler: GradientScheduler,
    samples: dict[str, RoundSample],
    clock: FakeClock,
) -> None:
    sample = samples["demo-strict-late"]
    create_round(scheduler, sample)
    submit_nodes(scheduler, sample, ("worker-a", "worker-b"))
    with pytest.raises(GradSyncError) as busy:
        scheduler.finalize(round_id=sample.round_id)
    assert busy.value.code == ErrorCode.E_BUSY
    assert scheduler.get_round_status(sample.round_id)["state"] == "COLLECTING"

    clock.advance(0.8)
    submit_nodes(scheduler, sample, ("worker-c",))
    status = scheduler.get_round_status(sample.round_id)
    assert status["state"] == "READY"
    assert [(item["node_id"], item["shard_id"]) for item in status["late_blocks"]] == [
        ("worker-c", shard_id) for shard_id in range(sample.shard_count)
    ]
    result = scheduler.finalize(round_id=sample.round_id)
    assert result["digest_sha256"] == sample.expected_digest()
    assert result["missing_nodes"] == []
    _ = clock


def test_strict_mode_degrades_only_after_max_wait(
    scheduler: GradientScheduler,
    samples: dict[str, RoundSample],
    clock: FakeClock,
) -> None:
    sample = samples["demo-strict-late"]
    create_round(scheduler, sample)
    submit_nodes(scheduler, sample, ("worker-a", "worker-b"))
    clock.advance(0.8)
    with pytest.raises(GradSyncError) as busy:
        scheduler.finalize(round_id=sample.round_id)
    assert busy.value.code == ErrorCode.E_BUSY

    clock.advance(sample.max_wait_seconds)
    result = scheduler.finalize(round_id=sample.round_id)
    assert result["code"] == "E_DEGRADED_ROUND"
    assert result["degraded"] is True
    assert result["missing_nodes"] == ["worker-c"]
    assert result["digest_sha256"] == sample.expected_digest(("worker-a", "worker-b"))


def test_strict_mode_max_wait_caps_late_blocks(
    scheduler: GradientScheduler,
    samples: dict[str, RoundSample],
    clock: FakeClock,
) -> None:
    sample = samples["demo-strict-late"]
    create_round(scheduler, sample)
    submit_nodes(scheduler, sample, ("worker-a", "worker-b"))
    clock.advance(sample.collect_seconds + sample.max_wait_seconds + 0.2)
    submit_nodes(scheduler, sample, ("worker-c",))
    status = scheduler.get_round_status(sample.round_id)
    assert status["state"] == "COLLECTING"
    assert len(status["late_blocks"]) == sample.shard_count

    result = scheduler.finalize(round_id=sample.round_id)
    assert result["code"] == "E_DEGRADED_ROUND"
    assert result["missing_nodes"] == ["worker-c"]
    assert result["digest_sha256"] == sample.expected_digest(("worker-a", "worker-b"))


def test_degraded_mode_uses_cutoff_snapshot_and_late_queue(
    scheduler: GradientScheduler,
    samples: dict[str, RoundSample],
    clock: FakeClock,
) -> None:
    sample = samples["demo-degraded"]
    create_round(scheduler, sample)
    submit_nodes(scheduler, sample, ("worker-a", "worker-b"))
    clock.advance(0.8)
    submit_nodes(scheduler, sample, ("worker-c",))
    status = scheduler.get_round_status(sample.round_id)
    assert status["received_blocks"] == sample.shard_count * 3
    assert len(status["late_blocks"]) == sample.shard_count

    result = scheduler.finalize(
        round_id=sample.round_id, policy=FinalizationPolicy.DEGRADED
    )
    assert result["code"] == "E_DEGRADED_ROUND"
    assert result["missing_nodes"] == ["worker-c"]
    assert result["digest_sha256"] == sample.expected_digest(("worker-a", "worker-b"))


def test_zero_weight_is_business_error_without_division(
    scheduler: GradientScheduler, samples: dict[str, RoundSample]
) -> None:
    sample = samples["demo-zero-weight"]
    create_round(scheduler, sample)
    submit_nodes(scheduler, sample, sample.worker_ids)
    with pytest.raises(GradSyncError) as error:
        scheduler.finalize(round_id=sample.round_id)
    assert error.value.code == ErrorCode.E_ZERO_WEIGHT
    status = scheduler.get_round_status(sample.round_id)
    assert status["state"] == "READY"
    assert status["last_error_code"] == "E_ZERO_WEIGHT"
    assert status["result"] is None


def test_invalid_transition_and_unknown_round_do_not_mutate_state(
    scheduler: GradientScheduler, samples: dict[str, RoundSample]
) -> None:
    sample = samples["demo-complete"]
    create_round(scheduler, sample)
    submit_nodes(scheduler, sample, sample.worker_ids)
    scheduler.finalize(round_id=sample.round_id)
    before = scheduler.get_round_status(sample.round_id)
    with pytest.raises(GradSyncError) as invalid:
        scheduler.abort(sample.round_id)
    assert invalid.value.code == ErrorCode.E_INVALID_TRANSITION
    after = scheduler.get_round_status(sample.round_id)
    assert after["state"] == before["state"]
    assert after["events"] == before["events"]

    with pytest.raises(GradSyncError) as missing:
        scheduler.get_round_status("not-a-round")
    assert missing.value.code == ErrorCode.E_ROUND_NOT_FOUND
    assert all(item["round_id"] != "not-a-round" for item in scheduler.store.list_rounds())


def test_concurrent_finalize_executes_once_and_reuses_result(
    scheduler: GradientScheduler, samples: dict[str, RoundSample]
) -> None:
    sample = samples["demo-complete"]
    create_round(scheduler, sample)
    submit_nodes(scheduler, sample, sample.worker_ids)
    barrier = threading.Barrier(8)

    def run_finalize() -> str:
        barrier.wait()
        return scheduler.finalize(round_id=sample.round_id)["digest_sha256"]

    with ThreadPoolExecutor(max_workers=8) as executor:
        digests = list(executor.map(lambda _: run_finalize(), range(8)))
    assert len(set(digests)) == 1
    assert digests[0] == sample.expected_digest()
    events = scheduler.get_events(sample.round_id)
    assert sum(1 for event in events if event["event_type"] == "ROUND_AGGREGATED") == 1


def test_different_rounds_collect_concurrently(
    scheduler: GradientScheduler, samples: dict[str, RoundSample]
) -> None:
    first = samples["demo-complete"]
    second = samples["demo-idempotent-a"]
    create_round(scheduler, first)
    create_round(scheduler, second)
    barrier = threading.Barrier(2)

    def submit_round(sample: RoundSample) -> None:
        barrier.wait()
        submit_nodes(scheduler, sample, sample.worker_ids)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(submit_round, sample) for sample in (first, second)]
        for future in futures:
            future.result()
    assert scheduler.get_round_status(first.round_id)["state"] == "READY"
    assert scheduler.get_round_status(second.round_id)["state"] == "READY"


class SimulatedCrash(RuntimeError):
    pass


def test_recovery_replays_pending_intent_from_sqlite(
    tmp_path: Path, clock: FakeClock, samples: dict[str, RoundSample]
) -> None:
    db_path = tmp_path / "crash.db"
    sample = samples["demo-crash"]

    def crash(point: str, facts: dict) -> None:
        if point == "before_aggregated_commit":
            raise SimulatedCrash(facts["intent_id"])

    first_scheduler = GradientScheduler(
        SQLiteStore(db_path), clock=clock, crash_hook=crash, allow_process_crash=False
    )
    create_round(first_scheduler, sample)
    submit_nodes(first_scheduler, sample, sample.worker_ids)
    with pytest.raises(SimulatedCrash):
        first_scheduler.finalize(round_id=sample.round_id)

    before = first_scheduler.get_round_status(sample.round_id)
    assert before["state"] == "READY"
    assert before["result"] is None
    pending = first_scheduler.store.get_pending_intents()
    assert len(pending) == 1
    assert pending[0]["round_id"] == sample.round_id

    recovered_scheduler = GradientScheduler(
        SQLiteStore(db_path), clock=clock, allow_process_crash=False
    )
    report = recovered_scheduler.recover_pending_intents()
    assert report["source"] == "startup_sqlite_scan"
    assert report["pending_intents_found"] == 1
    assert report["entries"][0]["outcome"] == "RECOVERED"
    result = recovered_scheduler.get_result(sample.round_id)
    assert result["digest_sha256"] == sample.expected_digest()
    status = recovered_scheduler.get_round_status(sample.round_id)
    assert status["state"] == "AGGREGATED"
    assert any(event["event_type"] == "RECOVERY_STARTED" for event in status["events"])
    aggregate_event = next(
        event for event in status["events"] if event["event_type"] == "ROUND_AGGREGATED"
    )
    assert aggregate_event["details"]["recovered"] is True


def test_sample_manifest_check_is_deterministic(tmp_path: Path) -> None:
    from gradsync.samples import write_sample_manifest

    first = tmp_path / "a.json"
    second = tmp_path / "b.json"
    write_sample_manifest(first)
    write_sample_manifest(second)
    assert first.read_bytes() == second.read_bytes()

