from __future__ import annotations

from pathlib import Path

from tests.conftest import make_engine


def test_arrival_and_id_order_converge_byte_for_byte(tmp_path: Path, demo_dataset: Path) -> None:
    valid_ids = ["txt-a", "txt-b", "txt-c", "txt-d", "txt-e"]
    arrival = make_engine(tmp_path / "arrival", demo_dataset)
    by_id = make_engine(tmp_path / "by-id", demo_dataset)
    result_arrival = arrival.backfill_all(
        text_ids=valid_ids,
        expected_sequence=0,
        batch_size=1,
        batch_prefix="arrival",
        order="arrival",
    )
    result_id = by_id.backfill_all(
        text_ids=valid_ids,
        expected_sequence=0,
        batch_size=1,
        batch_prefix="id",
        order="id",
    )
    assert result_arrival["committed_batch_count"] == 5
    assert result_id["committed_batch_count"] == 5
    assert arrival.workspace.index_path.read_bytes() == by_id.workspace.index_path.read_bytes()
    assert arrival.workspace.vectors_path.read_bytes() == by_id.workspace.vectors_path.read_bytes()
    index_a = arrival.index()
    index_b = by_id.index()
    assert index_a["split_history"] == index_b["split_history"]
    assert index_a["ownership_summary"] == index_b["ownership_summary"]
    assert index_a["routes"] == index_b["routes"]
    assert index_a["shards"] != []
    assert any(len(item["moved_entries"]) > 0 for item in index_a["split_history"])
    assert any(item["kind"] == "fallback" for item in index_a["routes"])
