import os

import pytest

from pkq.columnar import SnapshotStore, load_manifest
from pkq.errors import SnapshotInvalid


def test_version_monotonic_and_atomic_swap(sample_root):
    store = SnapshotStore()
    s1 = store.load(os.path.join(sample_root, "snapshot_v1"))
    assert store.current().version == 1
    with pytest.raises(SnapshotInvalid):
        store.load(os.path.join(sample_root, "snapshot_v1"))  # 版本未递增
    assert store.current().version == 1  # 旧快照保留
    s2 = store.load(os.path.join(sample_root, "snapshot_v2"))
    assert store.current().version == 2
    assert store.current() is s2 and store.current() is not s1  # 整体替换


def test_manifest_checksum_tamper(sample_root, tmp_path):
    import json, shutil
    src = os.path.join(sample_root, "snapshot_v1")
    dst = str(tmp_path / "tampered")
    shutil.copytree(src, dst)
    mpath = os.path.join(dst, "manifest.json")
    m = json.load(open(mpath))
    m["generated_at"] = "tampered"
    json.dump(m, open(mpath, "w"))
    with pytest.raises(SnapshotInvalid):
        load_manifest(dst)


def test_block_index_order_validated(sample_root, tmp_path):
    import json, shutil
    from pkq.columnar import write_snapshot
    src = os.path.join(sample_root, "snapshot_v1")
    dst = str(tmp_path / "overlap")
    shutil.copytree(src, dst)
    m = json.load(open(os.path.join(dst, "manifest.json")))
    blocks = m["blocks"]
    blocks[1]["pk_min"] = blocks[0]["pk_max"]  # 制造区间重叠
    write_snapshot(dst, m["version"], m["generated_at"], m["columns"], blocks)
    # write_snapshot 重算摘要，但区间重叠应在加载时被拒
    with pytest.raises(SnapshotInvalid):
        load_manifest(dst)


def test_locate_blocks(sample_root):
    store = SnapshotStore()
    snap = store.load(os.path.join(sample_root, "snapshot_v1"))
    hit = snap.locate_blocks([0, 5005, 11000])
    assert [b.block_id for b in hit] == [0, 5, 11]
    assert snap.locate_blocks([999999]) == []
