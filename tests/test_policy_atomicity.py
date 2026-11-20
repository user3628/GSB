from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from workbench.errors import WorkbenchError
from workbench.policy import CachePolicy
from workbench.store import Store

ROOT = Path(__file__).resolve().parents[1]


def test_stale_and_invalid_snapshots_leave_current_policy_unchanged(tmp_path: Path) -> None:
    store = Store(tmp_path / "policy.db")
    store.initialize()
    for node in ("edge-a", "edge-b", "edge-c"):
        store.ensure_node(node)
    v1 = json.loads((ROOT / "samples" / "policy_v1.json").read_text(encoding="utf-8"))
    v2 = json.loads((ROOT / "samples" / "policy_v2.json").read_text(encoding="utf-8"))
    store.submit_policy(CachePolicy.from_snapshot(v1, ["edge-a", "edge-b", "edge-c"]))

    with pytest.raises(WorkbenchError) as stale:
        store.submit_policy(CachePolicy.from_snapshot(v1, ["edge-a", "edge-b", "edge-c"]))
    assert stale.value.code == "E_STALE_SNAPSHOT"
    assert store.get_current_policy().version == 1

    invalid = copy.deepcopy(v2)
    invalid["digest"] = "0" * 64
    with pytest.raises(WorkbenchError) as invalid_error:
        CachePolicy.from_snapshot(invalid, ["edge-a", "edge-b", "edge-c"])
    assert invalid_error.value.code == "E_SNAPSHOT_INVALID"
    assert store.get_current_policy().version == 1

    store.submit_policy(CachePolicy.from_snapshot(v2, ["edge-a", "edge-b", "edge-c"]))
    current = store.get_current_policy()
    assert current.version == 2
    assert current.vary == ("accept-language", "x-tenant")
    assert current.ttl_seconds == 8.0


def test_policy_replacement_is_whole_snapshot_not_field_merge(tmp_path: Path) -> None:
    store = Store(tmp_path / "policy2.db")
    store.initialize()
    for node in ("edge-a", "edge-b", "edge-c"):
        store.ensure_node(node)
    v1 = json.loads((ROOT / "samples" / "policy_v1.json").read_text(encoding="utf-8"))
    v2 = json.loads((ROOT / "samples" / "policy_v2.json").read_text(encoding="utf-8"))
    p1 = CachePolicy.from_snapshot(v1, ["edge-a", "edge-b", "edge-c"])
    store.submit_policy(p1)
    p2 = CachePolicy.from_snapshot(v2, ["edge-a", "edge-b", "edge-c"])
    result = store.submit_policy(p2)
    assert result["previous_version"] == 1
    current = store.get_current_policy()
    assert current.normalized_snapshot == p2.normalized_snapshot
    assert current.ttl_seconds == 8.0
    assert current.stale_while_revalidate_seconds == 4.0
    assert current.purge_propagation_window_ms == 800.0

