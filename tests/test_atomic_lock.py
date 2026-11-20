from __future__ import annotations

import json
import threading

import pytest

from gsb.errors import WorkbenchError
from gsb.util import atomic_write_json, read_json
from gsb.workspace import Workspace


def test_atomic_write_replaces_complete_json_without_temp_files(tmp_path):
    target = tmp_path / "nested" / "record.json"
    atomic_write_json(target, {"version": 1, "payload": "a" * 4096})
    atomic_write_json(target, {"version": 2, "payload": "b" * 4096})
    assert read_json(target) == {"version": 2, "payload": "b" * 4096}
    assert list(target.parent.iterdir()) == [target]


def test_concurrent_publish_uses_lock_and_monotonic_sequence(make_workspace, small_dataset):
    workspace = make_workspace("concurrent_publish")
    workspace.import_dataset(small_dataset)
    barrier = threading.Barrier(2)
    results: list[tuple[str, int]] = []

    def publish(index: int) -> None:
        contender = Workspace(
            workspace.project_root,
            state_dir=workspace.state_dir,
            data_dir=workspace.data_dir,
        )
        barrier.wait()
        try:
            record = contender.publish_checkpoint(
                stage="binning",
                input_fingerprint=f"fingerprint-{index}",
                upstream_fingerprint=None,
                config_version="config-test",
                payload={"writer": index},
                accumulator={},
            )
            results.append(("ok", int(record["seq"])))
        except WorkbenchError as exc:
            results.append((exc.code, 0))

    threads = [threading.Thread(target=publish, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(results) == [("E_STALE_CHECKPOINT", 0), ("ok", 1)]
    assert workspace.status()["seq"] == 1
    assert json.loads(workspace.registry_path.read_text(encoding="utf-8"))["seq"] == 1
