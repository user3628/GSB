from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from app.errors import BusinessError
from app.service import TieringService


def test_preview_is_deterministic_and_unique(service: TieringService):
    first = service.preview_batch(1)
    second = service.preview_batch(1)
    assert first["candidates_total"] == 9
    assert first["selected_count"] == 9
    assert first["truncated_count"] == 0
    ids = [candidate["shard_id"] for candidate in first["selected"]]
    assert len(ids) == len(set(ids))
    assert ids == [candidate["shard_id"] for candidate in second["selected"]]
    assert ids == [
        "shard-011",
        "shard-005",
        "shard-010",
        "shard-007",
        "shard-012",
        "shard-008",
        "shard-004",
        "shard-006",
        "shard-002",
    ]


def test_batch_limit_reports_truncated_candidates(service: TieringService):
    service.adjust_snapshot(base_version=1, version=2, batch_limit=3)
    preview = service.preview_batch(2)
    assert preview["selected_count"] == 3
    assert preview["truncated_count"] == 6
    assert {item["truncation_reason"] for item in preview["truncated"]} == {"BATCH_LIMIT"}


def test_target_capacity_reports_shortfall(service: TieringService):
    occupancy = service.get_occupancy()
    hot = occupancy["layers"][0]
    capacity = hot["used_bytes"] + 400 * 1024**2
    service.adjust_snapshot(
        base_version=1,
        version=2,
        layer_capacity_updates={hot["layer_id"]: capacity},
    )
    preview = service.preview_batch(2)
    capacity_truncations = [
        item for item in preview["truncated"] if item["truncation_reason"] == "TARGET_CAPACITY"
    ]
    assert capacity_truncations
    assert any(item["shard_id"] == "shard-007" for item in capacity_truncations)
    assert all(item["detail"]["shortfall_bytes"] > 0 for item in capacity_truncations)


def test_batch_moves_shards_and_updates_occupancy_atomically(service: TieringService):
    before = service.get_occupancy()
    batch = service.execute_batch("batch-full", 1)
    assert batch["state"] == "COMMITTED"
    assert batch["replayed"] is False
    assert batch["conclusion_matches_items"] is True
    assert batch["result"]["moved_count"] == 9
    assert len(batch["items"]) == 9
    assert all(item["state"] == "COMMITTED" for item in batch["items"])
    assert all(item["last_changed_at"] for item in batch["items"])
    assert len(batch["transitions"]) == 27

    after = service.get_occupancy()
    assert after["totals"]["used_bytes"] == before["totals"]["used_bytes"]
    assert after["totals"]["shard_count"] == before["totals"]["shard_count"]
    assert [layer["used_bytes"] for layer in after["layers"]] == [
        layer["used_bytes"] for layer in batch["result"]["layer_after"]
    ]
    shards = service.list_shards(limit=100)["shards"]
    for layer in after["layers"]:
        layer_shards = [shard for shard in shards if shard["layer_id"] == layer["layer_id"]]
        assert layer["used_bytes"] == sum(shard["size_bytes"] for shard in layer_shards)
        assert layer["shard_count"] == len(layer_shards)
    assert sum(layer["used_bytes"] for layer in after["layers"]) == after["totals"]["used_bytes"]


def test_batch_replay_and_conflict(service: TieringService):
    first = service.execute_batch("idempotent", 1)
    second = service.execute_batch("idempotent", 1)
    assert first["replayed"] is False
    assert second["replayed"] is True
    assert second["result"] == first["result"]
    with pytest.raises(BusinessError) as raised:
        service.execute_batch("idempotent", 99)
    assert raised.value.code == "E_CONFLICTING_BATCH"


def test_empty_candidate_batch_is_committed_not_error(service: TieringService):
    service.execute_batch("first", 1)
    preview = service.preview_batch(1)
    assert preview["candidates_total"] == 0
    empty = service.execute_batch("empty", 1)
    assert empty["state"] == "COMMITTED"
    assert empty["result"]["empty"] is True
    assert empty["result"]["moved_count"] == 0
    assert empty["items"] == []


def test_concurrent_duplicate_batch_executes_once(service: TieringService):
    def run():
        return service.execute_batch("concurrent", 1)

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: run(), range(2)))
    assert sorted(result["replayed"] for result in results) == [False, True]
    assert results[0]["result"] == results[1]["result"]
    assert len(service.get_batch("concurrent")["transitions"]) == 27


def test_readers_observe_only_old_or_new_occupancy(service: TieringService):
    before = {
        layer["layer_id"]: (layer["used_bytes"], layer["shard_count"])
        for layer in service.get_occupancy()["layers"]
    }
    observed_before_commit = []

    def reader():
        seen = []
        for _ in range(80):
            occupancy = service.get_occupancy()
            seen.append(
                {
                    layer["layer_id"]: (layer["used_bytes"], layer["shard_count"])
                    for layer in occupancy["layers"]
                }
            )
        return seen

    with ThreadPoolExecutor(max_workers=5) as executor:
        read_futures = [executor.submit(reader) for _ in range(4)]
        batch_future = executor.submit(service.execute_batch, "reads", 1)
        batch = batch_future.result()
        observed_before_commit = [item for future in read_futures for item in future.result()]

    after = {
        layer["layer_id"]: (layer["used_bytes"], layer["shard_count"])
        for layer in service.get_occupancy()["layers"]
    }
    assert before != after
    assert batch["state"] == "COMMITTED"
    for observation in observed_before_commit:
        assert observation in (before, after)
