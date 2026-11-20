from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from gemm_lab.blocking import BlockPlan
from gemm_lab.config_store import VersionedConfigStore
from gemm_lab.errors import BusinessCode, GemmError


PLAN = BlockPlan(mr=8, nr=16, mc=64, kc=32, nc=64)


def publish(store: VersionedConfigStore, fingerprint: str, *, expected_seq: int | None = None):
    return store.publish(
        fingerprint=fingerprint,
        plan=PLAN,
        sum_strategy="pairwise",
        cache_budget={"l1_bytes": 32768, "l2_bytes": 1048576},
        selection_source="auto",
        expected_seq=expected_seq,
    )


def test_normal_progression_and_fingerprint_query(tmp_path: Path):
    store = VersionedConfigStore(tmp_path / "config_store")
    first = publish(store, "fingerprint-a")
    second = publish(store, "fingerprint-b")
    assert (first.seq, second.seq) == (1, 2)
    latest = store.load_latest()
    assert latest is not None and latest.seq == 2
    assert store.load_latest(expected_fingerprint="fingerprint-b").seq == 2
    state = store.verify_state()
    assert state.status == "valid" and state.anchor_seq == 2 and state.head_seq == 2


def test_stale_publish_requires_exact_sequence(tmp_path: Path):
    store = VersionedConfigStore(tmp_path / "config_store")
    publish(store, "a")
    with pytest.raises(GemmError) as error:
        publish(store, "b", expected_seq=0)
    assert error.value.code == BusinessCode.E_STALE_PUBLISH
    assert error.value.details["reason"] == "sequence_conflict"


def test_fingerprint_mismatch_is_stale_config(tmp_path: Path):
    store = VersionedConfigStore(tmp_path / "config_store")
    publish(store, "actual")
    with pytest.raises(GemmError) as error:
        store.load_latest(expected_fingerprint="different")
    assert error.value.code == BusinessCode.E_STALE_CONFIG
    assert error.value.details["reason"] == "fingerprint_mismatch"


def test_whole_store_rollback_is_distinguished(tmp_path: Path):
    root = tmp_path / "config_store"
    store = VersionedConfigStore(root)
    publish(store, "v1")
    publish(store, "v2")
    backup = tmp_path / "snapshot_v2"
    shutil.copytree(root, backup)
    publish(store, "v3")
    shutil.rmtree(root)
    shutil.copytree(backup, root)
    with pytest.raises(GemmError) as error:
        VersionedConfigStore(root).verify_state()
    assert error.value.code == BusinessCode.E_STALE_CONFIG
    assert error.value.details["reason"] == "rollback"
    assert error.value.details["anchor_seq"] == 3
    assert error.value.details["latest_record_seq"] == 2


def test_tampered_record_digest_is_distinguished(tmp_path: Path):
    root = tmp_path / "config_store"
    store = VersionedConfigStore(root)
    publish(store, "v1")
    record_path = sorted((root / "records").glob("*.json"))[0]
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["payload"]["plan"]["kc"] = 999
    record_path.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(GemmError) as error:
        store.verify_state()
    assert error.value.code == BusinessCode.E_STALE_CONFIG
    assert error.value.details["reason"] == "tampered_record"

