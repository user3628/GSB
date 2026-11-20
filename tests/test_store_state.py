from __future__ import annotations

import json
from pathlib import Path

import pytest

from workbench.errors import WorkbenchError
from workbench.policy import CachePolicy
from workbench.store import Store

ROOT = Path(__file__).resolve().parents[1]


def _policy(store: Store, version: int = 1) -> CachePolicy:
    raw = json.loads((ROOT / "samples" / "policy_v1.json").read_text(encoding="utf-8"))
    raw["version"] = version
    raw.pop("digest", None)
    policy = CachePolicy.from_snapshot(raw, ["edge-a", "edge-b", "edge-c"])
    store.submit_policy(policy)
    return policy


def _entry(policy: CachePolicy, key: str = "key-1") -> dict:
    return {
        "cache_key": key,
        "key_canonical": '{"path":"/a"}',
        "canonical_path": "/a",
        "canonical_query": "",
        "vary": {},
        "body": b"body",
        "content_type": "text/plain",
        "etag": '"etag-1"',
        "origin_version": "version-1",
        "stored_at": 1000.0,
        "fresh_until": 2000.0,
        "stale_until": 3000.0,
        "policy_version": policy.version,
        "tags": ["tag-a"],
        "route_key": "/a",
    }


def test_invalid_transition_does_not_mutate_entry_or_indexes(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.db")
    store.initialize()
    store.ensure_node("edge-a")
    policy = _policy(store)
    epoch = store.node_epoch("edge-a")
    store.replace_entry(
        node_id="edge-a",
        entry=_entry(policy),
        intent_id=None,
        expected_epoch=epoch,
        source_request_id="setup",
    )
    store.transition_entry(
        node_id="edge-a", cache_key="key-1", from_state="FRESH", to_state="PURGED", reason="test"
    )
    before = store.get_entry("edge-a", "key-1")
    with pytest.raises(WorkbenchError) as caught:
        store.transition_entry(
            node_id="edge-a", cache_key="key-1", from_state="PURGED", to_state="FRESH", reason="illegal"
        )
    after = store.get_entry("edge-a", "key-1")
    assert caught.value.code == "E_INVALID_TRANSITION"
    assert before["state"] == after["state"] == "PURGED"
    with store.read() as connection:
        assert connection.execute("SELECT COUNT(*) FROM route_index").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM tag_index").fetchone()[0] == 0


def test_recovery_rolls_back_intent_and_normalizes_revalidating_entry(tmp_path: Path) -> None:
    store = Store(tmp_path / "recovery.db")
    store.initialize()
    store.ensure_node("edge-a")
    policy = _policy(store)
    epoch = store.node_epoch("edge-a")
    entry = _entry(policy)
    entry.update({"fresh_until": 1.0, "stale_until": 2.0})
    store.replace_entry(
        node_id="edge-a",
        entry=entry,
        intent_id=None,
        expected_epoch=epoch,
        source_request_id="setup",
    )
    store.transition_entry(
        node_id="edge-a", cache_key="key-1", from_state="FRESH", to_state="REVALIDATING", reason="test"
    )
    intent_id = store.create_intent(
        kind="ORIGIN_FETCH",
        request_id="crashed-request",
        node_id="edge-a",
        cache_key="key-1",
        payload={"path": "/a"},
    )
    report = store.recover_on_startup()
    assert report["intents_scanned"] == 1
    assert report["intents_rolled_back"] == 1
    with store.read() as connection:
        intent = connection.execute("SELECT status FROM intents WHERE intent_id = ?", (intent_id,)).fetchone()
        assert intent["status"] == "ROLLED_BACK"
    recovered = store.get_entry("edge-a", "key-1")
    assert recovered["state"] == "STALE"
    assert recovered["etag"] == '"etag-1"'
    latest = store.latest_recovery_report()
    assert latest is not None
    assert latest["intents_rolled_back"] == 1
    assert latest["facts"]["post_recovery"]["complete_cache_entries"][0]["etag"] == '"etag-1"'

