from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier
import time

import pytest

from prep_pipeline.errors import PipelineError
from prep_pipeline.service import PipelineService

from tests.helpers import batch_request, make_settings


def test_same_batch_id_executes_once_and_waiters_reuse_result(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, stage_delay=0.08)
    service = PipelineService(settings)
    service.bootstrap_policies(
        [settings.data_dir / "policy_v1.json", settings.data_dir / "policy_v2.json"]
    )
    request = batch_request("same-batch")
    barrier = Barrier(2)

    def submit():
        barrier.wait()
        return service.submit_batch(request)

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = list(pool.map(lambda _: submit(), range(2)))

    assert first["status"] == second["status"] == "committed"
    assert first["attempt_id"] == second["attempt_id"]
    assert first["intent_id"] == second["intent_id"]
    assert len(service.db.get_stage_facts("same-batch")) == 5
    assert len(service.db.get_shard_rows("same-batch")) == 3
    with service.db.read_transaction() as conn:
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM intents WHERE batch_id = 'same-batch'"
        ).fetchone()["n"] == 1
        assert conn.execute(
            "SELECT COUNT(*) AS n FROM batches WHERE batch_id = 'same-batch'"
        ).fetchone()["n"] == 1


def test_same_policy_version_runs_are_serialized(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, stage_delay=0.08)
    service = PipelineService(settings)
    service.bootstrap_policies(
        [settings.data_dir / "policy_v1.json", settings.data_dir / "policy_v2.json"]
    )
    barrier = Barrier(2)

    def submit(batch_id: str):
        barrier.wait()
        return service.submit_batch(batch_request(batch_id))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(submit, "serial-a"), pool.submit(submit, "serial-b")]
        results = [future.result() for future in futures]

    assert all(result["status"] == "committed" for result in results)
    facts_a = service.db.get_stage_facts("serial-a")
    facts_b = service.db.get_stage_facts("serial-b")
    if facts_a[0]["started_at"] <= facts_b[0]["started_at"]:
        assert facts_a[-1]["completed_at"] <= facts_b[0]["started_at"]
    else:
        assert facts_b[-1]["completed_at"] <= facts_a[0]["started_at"]
    assert service.db.get_shard_rows("serial-a")
    assert service.db.get_shard_rows("serial-b")


def test_readers_see_only_complete_snapshots_during_a_write(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, stage_delay=0.08)
    service = PipelineService(settings)
    service.bootstrap_policies(
        [settings.data_dir / "policy_v1.json", settings.data_dir / "policy_v2.json"]
    )
    before = service.submit_batch(batch_request("reader-before"))
    before_digests = {stage["stage"]: stage["output_digest"] for stage in before["stages"]}

    with ThreadPoolExecutor(max_workers=1) as writer_pool:
        writer = writer_pool.submit(service.submit_batch, batch_request("reader-inflight"))
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                if service.db.get_batch("reader-inflight")["status"] == "running":
                    break
            except PipelineError as exc:
                if exc.code != "E_BATCH_NOT_FOUND":
                    raise
            time.sleep(0.01)
        assert service.db.get_batch("reader-inflight")["status"] == "running"

        with ThreadPoolExecutor(max_workers=6) as readers:
            reads = list(readers.map(lambda _: service.get_batch("reader-before"), range(12)))
        assert all(
            {stage["stage"]: stage["output_digest"] for stage in item["stages"]}
            == before_digests
            for item in reads
        )
        with pytest.raises(PipelineError) as hidden:
            service.get_batch("reader-inflight")
        assert hidden.value.code == "E_BATCH_NOT_COMMITTED"
        completed = writer.result(timeout=30)
    assert completed["status"] == "committed"
    assert service.get_batch("reader-inflight")["status"] == "committed"
