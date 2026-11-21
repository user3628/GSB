from __future__ import annotations

import copy

import pytest

from app.errors import BusinessError
from tests.conftest import read_json


def test_main_trace_adjudicates_all_strategies(loaded_engine, sample_trace) -> None:
    result = loaded_engine.submit_trace(sample_trace)
    assert result["state"] == "COMMITTED"
    assert result["summary"] == {
        "request_count": 9,
        "success_count": 5,
        "failed_count": 4,
        "pending_count": 0,
        "strategy_counts": {
            "DIRECT_FAIL": 4,
            "DIRECT_READ": 1,
            "EC_REBUILD": 3,
            "REPLICA_FILL": 1,
        },
        "error_counts": {
            "E_LATENCY_GUARD": 1,
            "E_REBUILD_SHED": 2,
            "E_UNRECOVERABLE": 1,
        },
        "consumed_bytes": 2048,
        "estimated_makespan_us": 2412,
    }
    by_id = {decision["request_id"]: decision for decision in result["decisions"]}
    assert by_id["req-alpha-ec"]["outcome"] == "SUCCESS"
    assert by_id["req-alpha-ec"]["successfully_rebuilt_shards"] == [2, 4]
    assert by_id["req-beta-replica"]["strategy"] == "REPLICA_FILL"
    assert by_id["req-beta-replica"]["replica_filled_shards"] == [1]
    assert by_id["req-beta-replica"]["read_result"]["served_shards"][1]["source"] == "REPLICA_FILL"
    assert by_id["req-delta-direct"]["strategy"] == "DIRECT_READ"
    assert by_id["req-delta-direct"]["read_result"]["status"] == "COMPLETE"
    assert by_id["req-alpha-ec"]["read_result"]["served_shards"][2]["source"] == "EC_REBUILD"
    assert by_id["req-gamma-unrecoverable"]["error"]["code"] == "E_UNRECOVERABLE"
    assert by_id["req-zeta-shed"]["error"]["code"] == "E_REBUILD_SHED"
    assert by_id["req-theta-latency"]["error"]["code"] == "E_LATENCY_GUARD"
    assert by_id["req-alpha-ec"]["shard_health"][4]["reason_codes"] == ["NODE_UNAVAILABLE"]
    gamma_health = {item["shard_index"]: item for item in by_id["req-gamma-unrecoverable"]["shard_health"]}
    assert gamma_health[0]["reason_codes"] == ["DIGEST_MISMATCH"]
    assert gamma_health[3]["reason_codes"] == ["DISK_FAULT"]
    assert gamma_health[5]["reason_codes"] == ["SHARD_MISSING"]
    assert result["budget"]["consumed_bytes"] == 2048
    assert len(result["budget"]["tasks"]) == 4
    assert {task["state"] for task in result["budget"]["tasks"]} == {"COMMITTED"}
    assert [item["request_id"] for item in result["read_results"]] == [
        decision["request_id"] for decision in result["decisions"]
    ]


def test_replay_digest_is_bit_identical(loaded_engine, sample_trace) -> None:
    first = loaded_engine.submit_trace(sample_trace)
    second = loaded_engine.submit_trace(sample_trace)
    assert second["decision_digest"] == first["decision_digest"]
    assert set(second["reused_request_ids"]) == {request["request_id"] for request in sample_trace["requests"]}
    assert second["budget"]["consumed_bytes"] == 0


def test_same_request_hash_is_independent_of_database_instance(tmp_path, sample_layout, sample_trace) -> None:
    from app.engine import RepairEngine
    from app.store import Store

    first_engine = RepairEngine(Store(tmp_path / "one.db"))
    second_engine = RepairEngine(Store(tmp_path / "two.db"))
    for engine in (first_engine, second_engine):
        engine.recover()
        engine.load_layout(sample_layout)
    first = first_engine.submit_trace(copy.deepcopy(sample_trace))
    second = second_engine.submit_trace(copy.deepcopy(sample_trace))
    assert first["decision_digest"] == second["decision_digest"]


def test_conflicting_duplicate_request_is_rejected(loaded_engine, sample_trace) -> None:
    loaded_engine.submit_trace(sample_trace)
    changed = copy.deepcopy(sample_trace)
    changed["requests"][0]["priority"] = 1
    with pytest.raises(BusinessError) as caught:
        loaded_engine.submit_trace(changed)
    assert caught.value.code == "E_CONFLICTING_REQUEST"


def test_queue_capacity_returns_queue_full(loaded_engine) -> None:
    result = loaded_engine.submit_trace(read_json("queue_trace.json"))
    assert result["summary"]["error_counts"] == {"E_REBUILD_QUEUE_FULL": 2}
    assert result["budget"]["task_count"] == 1
    codes = [decision["error"]["code"] for decision in result["decisions"] if decision["error"]]
    assert codes == ["E_REBUILD_QUEUE_FULL", "E_REBUILD_QUEUE_FULL"]


def test_time_budget_sheds_with_explicit_basis(loaded_engine, sample_trace) -> None:
    constrained = copy.deepcopy(sample_trace)
    constrained["options"] = {
        "round_time_budget_us": 1000,
        "round_byte_budget": 4096,
        "rebuild_concurrency": 2,
        "rebuild_queue_capacity": 4,
    }
    result = loaded_engine.submit_trace(constrained)
    alpha = next(item for item in result["decisions"] if item["request_id"] == "req-alpha-ec")
    assert alpha["error"]["code"] == "E_REBUILD_SHED"
    assert alpha["basis"]["rule"] == "TIME_BUDGET_SHED"
    assert alpha["basis"]["estimated_makespan_us"] == 1206
    assert result["budget"]["task_count"] == 0


def test_reconstruct_mismatch_charges_budget_and_reports_both_digests(loaded_engine) -> None:
    result = loaded_engine.submit_trace(read_json("mismatch_trace.json"))
    decision = result["decisions"][0]
    assert decision["strategy"] == "EC_REBUILD"
    assert decision["outcome"] == "FAILED"
    assert decision["error"]["code"] == "E_RECONSTRUCT_MISMATCH"
    assert decision["error"]["details"]["shard_index"] == 1
    assert decision["error"]["details"]["expected_digest"] == "sha256:" + "0" * 64
    assert decision["error"]["details"]["actual_digest"].startswith("sha256:")
    assert result["budget"]["consumed_bytes"] == 384


def test_unrecoverable_request_never_creates_a_task(loaded_engine, sample_trace) -> None:
    result = loaded_engine.submit_trace(sample_trace)
    gamma = next(item for item in result["decisions"] if item["request_id"] == "req-gamma-unrecoverable")
    assert gamma["error"]["code"] == "E_UNRECOVERABLE"
    assert all(task["object_id"] != "obj-gamma" for task in result["budget"]["tasks"])


def test_persisted_shard_result_is_reused_by_a_new_request(loaded_engine, sample_trace) -> None:
    loaded_engine.submit_trace(sample_trace)
    retry = copy.deepcopy(read_json("recovery_trace.json"))
    result = loaded_engine.submit_trace(retry)
    decision = result["decisions"][0]
    assert decision["outcome"] == "SUCCESS"
    assert sorted(decision["reused_reconstructed_shards"]) == [2, 4]
    assert result["budget"]["task_count"] == 0
    assert result["budget"]["consumed_bytes"] == 0
