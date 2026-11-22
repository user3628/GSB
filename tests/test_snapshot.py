import pytest

from graphstore.errors import SnapshotInvalidError
from graphstore.snapshot import load_snapshot_file


def test_checksum_and_version(sample_path):
    snapshot = load_snapshot_file(sample_path / "snapshot_v1.json")
    assert snapshot.version == 1
    assert len(snapshot.nodes) == 10
    assert len(snapshot.edges) == 19


def test_bad_checksum(sample_path):
    with pytest.raises(SnapshotInvalidError) as exc:
        load_snapshot_file(sample_path / "snapshot_bad_checksum.json")
    assert exc.value.code == "E_SNAPSHOT_INVALID"
    assert "actual" in exc.value.context


def test_version_monotonic_and_old_snapshot_kept(engine, sample_path):
    before = engine.current_version
    v1_view = engine.registry.acquire(1)
    with pytest.raises(SnapshotInvalidError):
        engine.load_snapshot(str(sample_path / "snapshot_bad_checksum.json"))
    assert engine.current_version == before
    assert engine.registry.acquire(1).version == 1
    engine.registry.release(v1_view)

    result = engine.load_snapshot(str(sample_path / "snapshot_v2.json"))
    assert result["version"] == 2
    assert engine.current_version == 2


def test_reload_same_version_rejected(engine, sample_path):
    with pytest.raises(SnapshotInvalidError):
        engine.load_snapshot(str(sample_path / "snapshot_v1.json"))
