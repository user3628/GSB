from __future__ import annotations

import json
import multiprocessing as mp
import os

import pytest

from posalign.canonical import atomic_write_json, digest_json
from posalign.errors import BusinessError, E_STALE_MAPPING
from posalign.mapping_store import MappingStore


def _core(tag: str) -> dict:
    return {
        "resolved": [{"old_tag": "x", "new_tag": tag}],
        "candidate_index": {},
        "diagnostics": [],
    }


def _publish(store: MappingStore, number: int) -> dict:
    return store.publish(
        _core(f"T{number}"),
        alias_fingerprint=f"alias-{number}",
        adjudication_fingerprint=f"adj-{number}",
        input_digest=f"input-{number}",
    )


def _mapping_publisher(workspace: str, index: int, queue) -> None:
    from posalign.errors import BusinessError
    from posalign.mapping_store import MappingStore

    try:
        MappingStore(workspace).publish(
            _core(f"T{index}"),
            alias_fingerprint=f"a{index}",
            adjudication_fingerprint=f"b{index}",
            input_digest=f"i{index}",
            sequence=1,
        )
        queue.put(("ok", index))
    except BusinessError as error:
        queue.put((error.code, index))


def test_monotonic_versions_and_normal_advance(tmp_path):
    store = MappingStore(tmp_path)
    first = _publish(store, 1)
    second = _publish(store, 2)
    third = _publish(store, 3)
    assert (first["version"], first["sequence"]) == (1, 1)
    assert (second["version"], second["sequence"]) == (2, 2)
    assert (third["version"], third["sequence"]) == (3, 3)
    validation = store.validate()
    assert validation.status == "valid"
    assert validation.version == 3
    assert (store.versions_dir / "00000003.json").is_file()


def test_rollback_is_rejected_and_old_content_preserved(tmp_path):
    store = MappingStore(tmp_path)
    _publish(store, 1)
    _publish(store, 2)
    current_before = store.current_path.read_bytes()
    old = (store.versions_dir / "00000001.json").read_bytes()
    store.current_path.write_bytes(old)
    with pytest.raises(BusinessError) as caught:
        store.validate()
    assert caught.value.code == E_STALE_MAPPING
    assert caught.value.details["classification"] == "rollback"
    with pytest.raises(BusinessError):
        _publish(store, 3)
    assert (store.versions_dir / "00000002.json").is_file()
    assert store.current_path.read_bytes() == old
    assert current_before != old


def test_digest_and_fingerprint_tampering_are_rejected(tmp_path):
    store = MappingStore(tmp_path / "digest")
    _publish(store, 1)
    current = json.loads(store.current_path.read_text(encoding="utf-8"))
    current["mapping_digest"] = "0" * 64
    atomic_write_json(store.current_path, current)
    with pytest.raises(BusinessError) as caught:
        store.validate()
    assert caught.value.details["classification"] == "tampered"
    assert "digest" in caught.value.details["reason"]

    store2 = MappingStore(tmp_path / "fingerprint")
    _publish(store2, 1)
    current = json.loads(store2.current_path.read_text(encoding="utf-8"))
    current["alias_fingerprint"] = "replaced"
    atomic_write_json(store2.current_path, current)
    with pytest.raises(BusinessError) as caught2:
        store2.validate()
    assert caught2.value.details["classification"] == "tampered"


def test_missing_anchor_and_stale_sequence(tmp_path):
    store = MappingStore(tmp_path)
    _publish(store, 1)
    store.anchor_path.unlink()
    with pytest.raises(BusinessError) as caught:
        store.validate()
    assert caught.value.details["classification"] == "tampered"

    store2 = MappingStore(tmp_path / "sequence")
    first = _publish(store2, 1)
    before = store2.current_path.read_bytes()
    with pytest.raises(BusinessError) as caught2:
        store2.publish(
            _core("T2"),
            alias_fingerprint="a",
            adjudication_fingerprint="b",
            input_digest="i",
            sequence=9,
        )
    assert caught2.value.code == E_STALE_MAPPING
    assert caught2.value.details["classification"] == "stale_sequence"
    assert store2.current_path.read_bytes() == before
    assert first["version"] == 1


def test_whole_mapping_replacement_is_detected_by_anchor(tmp_path):
    store = MappingStore(tmp_path)
    _publish(store, 1)
    current = json.loads(store.current_path.read_text(encoding="utf-8"))
    current["mapping"]["resolved"][0]["new_tag"] = "REPLACED"
    current["mapping_digest"] = digest_json(current["mapping"])
    ledger = json.loads(store.ledger_path.read_text(encoding="utf-8"))
    ledger["latest_mapping_digest"] = current["mapping_digest"]
    ledger["history"][-1]["mapping_digest"] = current["mapping_digest"]
    atomic_write_json(store.current_path, current)
    atomic_write_json(store.versions_dir / "00000001.json", current)
    atomic_write_json(store.ledger_path, ledger)
    with pytest.raises(BusinessError) as caught:
        store.validate()
    assert caught.value.code == E_STALE_MAPPING
    assert caught.value.details["classification"] == "tampered"
    assert "anchor" in caught.value.details["reason"]


def test_truncated_current_json_is_stale_mapping(tmp_path):
    store = MappingStore(tmp_path)
    _publish(store, 1)
    store.current_path.write_bytes(b"{truncated")
    with pytest.raises(BusinessError) as caught:
        store.validate()
    assert caught.value.code == E_STALE_MAPPING
    assert caught.value.details["classification"] == "tampered"
    assert (store.versions_dir / "00000001.json").is_file()


def test_version_gap_and_historical_archive_tamper_are_rejected(tmp_path):
    store = MappingStore(tmp_path / "gap")
    _publish(store, 1)
    _publish(store, 2)
    (store.versions_dir / "00000001.json").unlink()
    with pytest.raises(BusinessError) as caught:
        store.validate()
    assert caught.value.code == E_STALE_MAPPING
    assert caught.value.details["classification"] == "tampered"

    store2 = MappingStore(tmp_path / "history")
    _publish(store2, 1)
    _publish(store2, 2)
    historical = json.loads((store2.versions_dir / "00000001.json").read_text(encoding="utf-8"))
    historical["alias_fingerprint"] = "tampered-history"
    atomic_write_json(store2.versions_dir / "00000001.json", historical)
    with pytest.raises(BusinessError) as caught2:
        store2.validate()
    assert caught2.value.code == E_STALE_MAPPING
    assert caught2.value.details["classification"] == "tampered"


def test_multi_process_mapping_sequence_is_serialized(tmp_path):
    context = mp.get_context("fork" if os.name != "nt" else "spawn")
    queue = context.Queue()
    processes = [
        context.Process(target=_mapping_publisher, args=(str(tmp_path), index, queue))
        for index in range(5)
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=15)
        assert process.exitcode == 0
    results = [queue.get(timeout=5) for _ in processes]
    assert len([item for item in results if item[0] == "ok"]) == 1
    assert len([item for item in results if item[0] == E_STALE_MAPPING]) == 4
    ledger = json.loads((tmp_path / "mapping" / "ledger.json").read_text(encoding="utf-8"))
    assert ledger["latest_version"] == 1
    assert ledger["latest_sequence"] == 1

