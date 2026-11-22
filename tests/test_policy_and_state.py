from __future__ import annotations

import copy
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from app.errors import BusinessError
from app.service import TieringService
from app.utils import snapshot_checksum

from .conftest import SAMPLES


def _snapshot_from_sample(**changes):
    payload = json.loads((SAMPLES / "policy_snapshot.json").read_text(encoding="utf-8"))
    payload = copy.deepcopy(payload)
    payload.pop("checksum", None)
    payload.update(changes)
    payload["checksum"] = snapshot_checksum(payload)
    return payload


def test_initial_occupancy_matches_shard_sum(service: TieringService):
    occupancy = service.get_occupancy()
    shards = service.list_shards(limit=100)["shards"]
    assert occupancy["snapshot_version"] == 1
    assert occupancy["totals"]["shard_count"] == 12
    assert occupancy["totals"]["used_bytes"] == sum(shard["size_bytes"] for shard in shards)
    for layer in occupancy["layers"]:
        layer_shards = [shard for shard in shards if shard["layer_id"] == layer["layer_id"]]
        assert layer["used_bytes"] == sum(shard["size_bytes"] for shard in layer_shards)
        assert layer["shard_count"] == len(layer_shards)


def test_invalid_checksum_keeps_old_snapshot(service: TieringService):
    before = service.get_occupancy()
    payload = _snapshot_from_sample(version=2, heat_threshold=200)
    payload["checksum"] = "0" * 64
    with pytest.raises(BusinessError) as raised:
        service.submit_snapshot(payload)
    assert raised.value.code == "E_SNAPSHOT_INVALID"
    after = service.get_occupancy()
    assert after["snapshot_version"] == 1
    assert after["totals"] == before["totals"]
    assert service.get_current_policy()["snapshot"]["heat_threshold"] == 128


def test_stale_snapshot_version_is_rejected(service: TieringService):
    payload = _snapshot_from_sample(version=1, heat_threshold=200)
    with pytest.raises(BusinessError) as raised:
        service.submit_snapshot(payload)
    assert raised.value.code == "E_STALE_SNAPSHOT"
    assert service.get_current_policy()["snapshot"]["heat_threshold"] == 128


def test_full_snapshot_replaces_the_whole_policy(service: TieringService):
    payload = _snapshot_from_sample(
        version=2,
        heat_threshold=64,
        batch_limit=7,
        cooldown_seconds=30,
        layers=[
            {"id": "hot-nvme", "capacity_bytes": 5 * 1024**3},
            {"id": "warm-ssd", "capacity_bytes": 9 * 1024**3},
            {"id": "cold-hdd", "capacity_bytes": 17 * 1024**3},
            {"id": "archive", "capacity_bytes": 1024**3},
        ],
    )
    response = service.submit_snapshot(payload)
    assert response["mutation"] == "FULL_REPLACEMENT"
    policy = service.get_current_policy()["snapshot"]
    assert policy["version"] == 2
    assert policy["heat_threshold"] == 64
    assert policy["batch_limit"] == 7
    assert policy["cooldown_seconds"] == 30
    assert [layer["id"] for layer in policy["layers"]] == [
        "hot-nvme",
        "warm-ssd",
        "cold-hdd",
        "archive",
    ]
    assert policy["layers"][0]["capacity_bytes"] == 5 * 1024**3
    assert [layer["layer_id"] for layer in service.get_occupancy()["layers"]] == [
        "hot-nvme",
        "warm-ssd",
        "cold-hdd",
        "archive",
    ]


def test_capacity_overflow_is_atomic(service: TieringService):
    occupancy = service.get_occupancy()
    hot = occupancy["layers"][0]
    with pytest.raises(BusinessError) as raised:
        service.adjust_snapshot(
            base_version=1,
            version=2,
            layer_capacity_updates={hot["layer_id"]: hot["used_bytes"] - 1},
            heat_threshold=200,
        )
    assert raised.value.code == "E_CAPACITY_OVERFLOW"
    assert raised.value.context["layers"][0]["overflow_bytes"] == 1
    policy = service.get_current_policy()
    assert policy["snapshot"]["version"] == 1
    assert policy["snapshot"]["heat_threshold"] == 128
    assert service.get_occupancy()["totals"] == occupancy["totals"]


def test_adjustment_recomputes_cascade(service: TieringService):
    before_policy = service.get_current_policy()
    before_shards = service.list_shards(limit=100)["shards"]
    assert sum(int(shard["candidate_eligible"]) for shard in before_shards) == 9
    response = service.adjust_snapshot(
        base_version=1,
        version=2,
        heat_threshold=96,
        cooldown_seconds=0,
    )
    assert response["accepted"] is True
    assert response["snapshot"]["version"] == 2
    assert response["cascade"]["candidate_inputs_recomputed_in_transaction"] is True
    after_policy = service.get_current_policy()
    assert after_policy["snapshot"]["version"] == 2
    assert after_policy["snapshot"]["heat_threshold"] == 96
    assert after_policy["runtime"]["heat_threshold"] == 96
    assert after_policy["cascade_generation"] == before_policy["cascade_generation"] + 1
    after_shards = service.list_shards(limit=100)["shards"]
    assert all(shard["policy_snapshot_version"] == 2 for shard in after_shards)
    assert sum(int(shard["candidate_eligible"]) for shard in after_shards) == 9
    with pytest.raises(BusinessError) as raised:
        service.preview_batch(1)
    assert raised.value.code == "E_STALE_SNAPSHOT"


def test_concurrent_adjustments_only_one_wins(service: TieringService):
    def adjust(threshold: int):
        return service.adjust_snapshot(
            base_version=1,
            version=2,
            heat_threshold=threshold,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(adjust, 96), executor.submit(adjust, 192)]
        results = []
        for future in futures:
            try:
                results.append(("ok", future.result()))
            except BusinessError as exc:
                results.append(("error", exc.code))

    assert sorted(result[0] for result in results) == ["error", "ok"]
    assert [result[1] for result in results if result[0] == "error"] == ["E_STALE_BASE"]
    policy = service.get_current_policy()
    assert policy["snapshot"]["version"] == 2
    assert policy["snapshot"]["heat_threshold"] in (96, 192)
    assert policy["runtime"]["heat_threshold"] == policy["snapshot"]["heat_threshold"]


def test_invalid_transition_has_no_persisted_effect(service: TieringService):
    before_epoch = service.get_state_epoch()
    with pytest.raises(BusinessError) as raised:
        service.check_transition("COMMITTED", "PENDING")
    assert raised.value.code == "E_INVALID_TRANSITION"
    assert raised.value.context["persisted_state_changed"] is False
    assert service.get_state_epoch() == before_epoch
    assert service.check_transition("PENDING", "MIGRATING")["allowed"] is True


def test_shard_policy_eligibility_is_materialized(service: TieringService):
    shards = service.list_shards(limit=100)["shards"]
    eligible = [shard for shard in shards if shard["candidate_eligible"] == 1]
    assert len(eligible) == 9
    detail = service.get_shard(eligible[0]["shard_id"])
    assert detail["policy_state"]["candidate_eligible"] == 1
    assert detail["policy_state"]["target_layer_id"] in ("hot-nvme", "warm-ssd", "cold-hdd")
    assert detail["policy_state"]["snapshot_version"] == 1


def test_safe_shutdown_and_recovery_reports_are_distinct(tmp_path: Path):
    db = tmp_path / "tiering.db"
    first = TieringService(db, samples_dir=SAMPLES)
    first.startup()
    safe = first.safe_shutdown("test_safe_exit")
    assert safe["report_type"] == "SAFE_SHUTDOWN"
    assert safe["forced_termination_detected"] is False
    assert safe["safe_exit_summary_available"] is True

    second = TieringService(db, samples_dir=SAMPLES, auto_seed=False)
    second.startup()
    report = second.get_recovery_report()
    assert report["latest_startup_recovery"] is None
    assert report["latest_safe_shutdown"]["report_id"] == safe["report_id"]
    assert report["latest_safe_shutdown"]["forced_termination_detected"] is False
    second.safe_shutdown("pytest_teardown")
