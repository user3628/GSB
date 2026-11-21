from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from prep_pipeline.errors import PipelineError
from prep_pipeline.service import PipelineService

from tests.helpers import load_policy, policy_variant


def test_stale_snapshot_and_stale_base(service: PipelineService) -> None:
    v1 = load_policy()
    assert service.submit_policy(v1)["version"] == 1

    with pytest.raises(PipelineError) as same:
        service.submit_policy(v1)
    assert same.value.code == "E_STALE_SNAPSHOT"

    assert service.submit_policy(policy_variant(version=2, base_version=1))["version"] == 2
    with pytest.raises(PipelineError) as older:
        service.submit_policy(v1)
    assert older.value.code == "E_STALE_SNAPSHOT"

    stale_base = policy_variant(
        version=3, base_version=1, generated_at="2026-03-02T00:00:00Z", seed=2002
    )
    with pytest.raises(PipelineError) as base:
        service.submit_policy(stale_base)
    assert base.value.code == "E_STALE_BASE"
    assert base.value.context["current_version"] == 2


def test_concurrent_same_base_only_one_increment_wins(service: PipelineService) -> None:
    service.submit_policy(load_policy())
    contenders = [
        policy_variant(
            version=2,
            base_version=1,
            generated_at="2026-03-01T00:00:00Z",
            seed=3001,
        ),
        policy_variant(
            version=2,
            base_version=1,
            generated_at="2026-03-01T00:00:01Z",
            seed=3002,
        ),
    ]
    barrier = Barrier(2)

    def submit(snapshot):
        barrier.wait()
        try:
            return ("ok", service.submit_policy(snapshot))
        except PipelineError as exc:
            return ("error", exc)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(submit, contenders))

    winners = [result for result in results if result[0] == "ok"]
    losers = [result for result in results if result[0] == "error"]
    assert len(winners) == 1
    assert len(losers) == 1
    assert losers[0][1].code == "E_STALE_BASE"
    assert service.db.policy_head() == 2


def test_policy_checksum_is_enforced(service: PipelineService) -> None:
    value = load_policy().model_dump(mode="json")
    value["checksum"] = "sha256:" + "0" * 64
    from prep_pipeline.policy import parse_policy_snapshot

    with pytest.raises(PipelineError) as exc:
        parse_policy_snapshot(value)
    assert exc.value.code == "E_SNAPSHOT_INVALID"
