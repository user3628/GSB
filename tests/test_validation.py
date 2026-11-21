from __future__ import annotations

import copy

import pytest

from app.errors import BusinessError


def test_layout_requires_every_k_plus_m_shard(engine, sample_layout) -> None:
    broken = copy.deepcopy(sample_layout)
    broken["objects"][0]["shards"].pop()
    with pytest.raises(BusinessError) as caught:
        engine.load_layout(broken)
    assert caught.value.code == "E_LAYOUT_INVALID"
    assert engine.list_layouts() == []


def test_layout_rejects_missing_object_id_atomically(engine, sample_layout) -> None:
    broken = copy.deepcopy(sample_layout)
    broken["objects"][0].pop("object_id")
    with pytest.raises(BusinessError) as caught:
        engine.load_layout(broken)
    assert caught.value.code == "E_LAYOUT_INVALID"
    assert engine.store.get_layout_document(sample_layout["layout_version"]) is None


def test_layout_rejects_k_plus_m_above_declared_limit(engine, sample_layout) -> None:
    broken = copy.deepcopy(sample_layout)
    broken["max_k_plus_m"] = 5
    with pytest.raises(BusinessError) as caught:
        engine.load_layout(broken)
    assert caught.value.code == "E_LAYOUT_INVALID"
    assert "k+m" in caught.value.message


def test_layout_digest_format_is_strict(engine, sample_layout) -> None:
    broken = copy.deepcopy(sample_layout)
    broken["objects"][0]["shards"][0]["digest"] = "md5:abc"
    with pytest.raises(BusinessError) as caught:
        engine.load_layout(broken)
    assert caught.value.code == "E_LAYOUT_INVALID"


def test_layout_shards_are_normalized_to_index_order(engine, sample_layout) -> None:
    shuffled = copy.deepcopy(sample_layout)
    shuffled["objects"][0]["shards"].reverse()
    summary = engine.load_layout(shuffled)
    assert summary["created"] is True


def test_non_monotonic_trace_rejects_entire_batch(loaded_engine, sample_trace) -> None:
    broken = copy.deepcopy(sample_trace)
    broken["requests"][2]["arrival_ms"] = 999
    with pytest.raises(BusinessError) as caught:
        loaded_engine.submit_trace(broken)
    assert caught.value.code == "E_TRACE_INVALID"
    assert loaded_engine.list_rounds() == []


def test_bitmap_outside_object_rejects_batch(loaded_engine, sample_trace) -> None:
    broken = copy.deepcopy(sample_trace)
    broken["requests"][0]["shard_bitmap"] = "0x100"
    with pytest.raises(BusinessError) as caught:
        loaded_engine.submit_trace(broken)
    assert caught.value.code == "E_TRACE_INVALID"


def test_unknown_object_rejects_batch(loaded_engine, sample_trace) -> None:
    broken = copy.deepcopy(sample_trace)
    broken["requests"][0]["object_id"] = "missing-object"
    with pytest.raises(BusinessError) as caught:
        loaded_engine.submit_trace(broken)
    assert caught.value.code == "E_TRACE_INVALID"


def test_negative_requested_shard_index_rejects_batch(loaded_engine, sample_trace) -> None:
    broken = copy.deepcopy(sample_trace)
    broken["requests"][0].pop("shard_bitmap")
    broken["requests"][0]["requested_shards"] = [-1]
    with pytest.raises(BusinessError) as caught:
        loaded_engine.submit_trace(broken)
    assert caught.value.code == "E_TRACE_INVALID"
