import pytest

from logindex.models import BatchState


def test_retimestamp_rebuilds_all_three_derivatives_atomically(engine, sample_lines):
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    engine.submit_batch("B2", sample_lines("batch2_append.txt"), base_version=1)
    before = engine.index_state()
    v_before = before["version"]

    result = engine.retimestamp("B1", 3, 1700000000250)
    assert result["changed"] is True
    assert result["rebuilt_partitions"] == ["auth-svc"]
    assert result["version"] == v_before + 1

    state = engine.index_state()
    auth_timeline = [r for r in state["timeline_index"] if r["service"] == "auth-svc"]
    assert all(r["built_version"] == result["version"] for r in auth_timeline)
    assert all(r["last_reason"] == "RETIMESTAMP" for r in auth_timeline)
    auth_class = [r for r in state["classification_index"] if r["service"] == "auth-svc"]
    assert all(r["updated_version"] == result["version"] for r in auth_class)
    auth_stats = [r for r in state["batch_stats"] if r["service"] == "auth-svc"]
    assert all(r["updated_version"] == result["version"] for r in auth_stats)

    moved = engine.query_records(start_ms=1700000000250, end_ms=1700000000250,
                                 service="auth-svc")
    assert len(moved) == 1 and moved[0]["line_no"] == 3


def test_retimestamp_history_supports_time_travel(engine, sample_lines):
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    old_view = engine.query_records(service="auth-svc", level="WARN")
    assert old_view[0]["timestamp_ms"] == 1700000001000
    engine.retimestamp("B1", 3, 1700000000250)
    historical = engine.query_records(service="auth-svc", level="WARN",
                                      as_of_version=1)
    assert historical[0]["timestamp_ms"] == 1700000001000
    current = engine.query_records(service="auth-svc", level="WARN")
    assert current[0]["timestamp_ms"] == 1700000000250


def test_invalidate_batch_cascades_and_rebuilds(engine, sample_lines):
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    engine.submit_batch("B2", sample_lines("batch2_append.txt"), base_version=1)
    mixed = engine.submit_batch("B3", sample_lines("batch3_mixed.txt"), base_version=2)
    affected = mixed.affected_services
    result = engine.invalidate_batch("B3")
    assert sorted(result["rebuilt_partitions"]) == sorted(affected)
    batch = next(b for b in engine.list_batches() if b["batch_id"] == "B3")
    assert batch["state"] == BatchState.INVALIDATED.value

    # B3 记录不再可见，但 B1/B2 仍可查
    visible = engine.query_records()
    assert all(r["batch_id"] != "B3" for r in visible)
    assert len(visible) == 18
    # 历史版本仍可回看 B3
    v3 = engine.query_records(as_of_version=3)
    assert any(r["batch_id"] == "B3" for r in v3)
    # 派生表中不再有 B3 的行
    state = engine.index_state()
    assert all(r["batch_id"] != "B3" for r in state["classification_index"])
    assert all(r["batch_id"] != "B3" for r in state["batch_stats"])


def test_visibility_only_complete_versions(engine, sample_lines):
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    state_v1 = engine.index_state(as_of_version=1)
    state_current = engine.index_state()
    assert state_v1["version"] == 1
    assert state_current["version"] == engine.current_version()
    with pytest.raises(Exception):
        engine.index_state(as_of_version=999)


def test_rebuild_reason_and_partition_listing(engine, sample_lines):
    engine.submit_batch("B1", sample_lines("batch1_full.txt"))
    result = engine.submit_batch("B3", sample_lines("batch3_mixed.txt"), base_version=1)
    assert result.path_mode == "REBUILD"
    assert result.rebuilt_partitions == ["gateway"]
    assert result.rebuild_reason is not None
