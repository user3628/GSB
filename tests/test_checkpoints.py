from __future__ import annotations

from pathlib import Path

from gridpipe.checkpoints import CheckpointStore


def _publish(store: CheckpointStore, shard: str, index: int, fingerprint: str):
    return store.publish(
        shard_id=shard,
        shard_index=index,
        stage="detection",
        input_fingerprint=fingerprint,
        config_version="cfg",
        artifact_bytes=(fingerprint + "\n").encode("utf-8"),
    )


def test_recovery_ignores_partial_corrupt_and_duplicate_files(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "work", "recovery-run")
    _publish(store, "shard-0001", 1, "fp-1")
    _publish(store, "shard-0002", 2, "fp-2")
    _publish(store, "shard-0003", 3, "fp-3")
    assert [record["sequence"] for record in store.load_valid_records()] == [1, 2, 3]

    (store.records_dir / "000000000004.json.tmp").write_text('{"sequence":4', encoding="utf-8")
    (store.records_dir / "000000000003-copy.json").write_text(
        (store.records_dir / "000000000003.json").read_text(encoding="utf-8"), encoding="utf-8"
    )
    assert [record["sequence"] for record in store.load_valid_records()] == [1, 2, 3]

    (store.records_dir / "000000000002.json").write_text('{"sequence":2', encoding="utf-8")
    assert [record["sequence"] for record in store.load_valid_records()] == [1]

    _publish(store, "shard-0002", 2, "fp-2")
    records = store.load_valid_records()
    assert [record["sequence"] for record in records] == [1, 2, 3]
    assert [record["shard_id"] for record in records] == ["shard-0001", "shard-0002", "shard-0003"]


def test_atomic_artifacts_and_sequence_files_exist(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "work", "atomic-run")
    record = _publish(store, "shard-0001", 1, "fp-1")
    assert (store.records_dir / "000000000001.json").is_file()
    assert (tmp_path / "work" / record["artifact_path"]).is_file()
    assert not list(store.records_dir.glob("*.tmp"))
