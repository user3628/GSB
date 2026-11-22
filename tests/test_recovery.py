from __future__ import annotations

from pathlib import Path

from mmbackfill.engine import Engine
from mmbackfill.errors import EngineError
from tests.conftest import make_engine


VALID_IDS = ["txt-a", "txt-b", "txt-c", "txt-d", "txt-e"]


def test_stop_between_batches_then_recover_matches_clean_run(tmp_path: Path, demo_dataset: Path) -> None:
    clean = make_engine(tmp_path / "clean", demo_dataset)
    clean.submit_batch(batch_id="clean", text_ids=VALID_IDS, expected_sequence=0, order="id")
    expected_index = clean.workspace.index_path.read_bytes()

    resumed = make_engine(tmp_path / "resumed", demo_dataset)
    resumed.submit_batch(batch_id="r1", text_ids=["txt-a", "txt-b"], expected_sequence=0, order="id")
    resumed.workspace.request_stop("test stop")
    caught = None
    try:
        resumed.submit_batch(batch_id="r2", text_ids=["txt-c"], expected_sequence=1, order="id")
    except EngineError as exc:
        caught = exc
    assert caught is not None and caught.code == "E_STOPPED"
    recovered = resumed.recover(clear_stop=True)
    assert recovered["reused_existing_index"] is True
    resumed.submit_batch(batch_id="r2", text_ids=["txt-c", "txt-d", "txt-e"], expected_sequence=1, order="id")
    assert resumed.workspace.index_path.read_bytes() == expected_index
    assert resumed.status()["last_batch_seq"] == 2


def test_corrupt_incomplete_and_duplicate_records_are_ignored(tmp_path: Path, demo_dataset: Path) -> None:
    engine = make_engine(tmp_path / "workspace", demo_dataset)
    engine.submit_batch(batch_id="b1", text_ids=["txt-a", "txt-b"], expected_sequence=0, order="id")
    engine.submit_batch(batch_id="b2", text_ids=["txt-c", "txt-d", "txt-e"], expected_sequence=1, order="id")
    expected_index = engine.workspace.index_path.read_bytes()
    (engine.workspace.records_dir / "batch-000003.json").write_bytes(b"")
    (engine.workspace.records_dir / "batch-000002-copy.json").write_text("duplicate-looking", encoding="utf-8")

    engine.workspace.state_path.unlink()
    engine.workspace.index_path.unlink()
    result = engine.recover(clear_stop=True)
    assert result["last_batch_seq"] == 2
    assert len(result["ignored_records"]) == 2
    assert engine.workspace.index_path.read_bytes() == expected_index
    assert [item["batch_seq"] for item in engine.batches()["batches"]] == [1, 2]


def test_recovery_skips_already_committed_batches(tmp_path: Path, demo_dataset: Path) -> None:
    engine = make_engine(tmp_path / "workspace", demo_dataset)
    engine.submit_batch(batch_id="b1", text_ids=["txt-a"], expected_sequence=0, order="id")
    result = engine.recover(clear_stop=True)
    assert result["reused_existing_index"] is True
    assert result["skipped_committed_batches"] == [1]
