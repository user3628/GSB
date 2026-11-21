from __future__ import annotations

import multiprocessing
from pathlib import Path
from typing import Any

import pytest

import headworkbench.canonical as canonical
from headworkbench.canonical import digest_object, read_json, write_json_file
from headworkbench.config import current_config_path, load_config
from headworkbench.demo import reset_stage_cache, reset_versions
from headworkbench.errors import WorkbenchError
from headworkbench.pipeline import Pipeline
from headworkbench.service import WorkbenchService
from headworkbench.versioning import VersionStore


def make_pipeline(root: Path) -> Pipeline:
    return Pipeline(root, load_config(current_config_path(root)))


def over_budget_widths(pipeline: Pipeline) -> dict[str, int]:
    return {str(head.head_id): 6 for head in pipeline.dataset.heads}


def test_atomic_commit_overbudget_rollback_and_stale(workspace: Path) -> None:
    store = VersionStore(workspace)
    pipeline = make_pipeline(workspace)
    suggested = pipeline.run("compact", mode="incremental")["payload"]["allocation"]["suggested_head_widths"]
    first = store.commit(
        pipeline,
        suggested,
        expected_base_version=0,
        window_name="compact",
        verify_full=True,
    )
    assert first["version"] == 1
    assert first["status"] == "active"
    assert first["validation"]["valid"] is True

    rolled_back = store.commit(
        pipeline,
        over_budget_widths(pipeline),
        expected_base_version=1,
        window_name="compact",
        verify_full=True,
    )
    assert rolled_back["version"] == 2
    assert rolled_back["status"] == "rolled_back"
    assert rolled_back["effective_head_widths"] == suggested
    assert {item["code"] for item in rolled_back["validation"]["errors"]} >= {
        "global_budget_exceeded",
        "per_head_tolerance_exceeded",
    }

    history = store.history()
    assert history["current_version"] == 2
    assert [item["version"] for item in history["versions"]] == [1, 2]
    assert history["versions"][1]["attempted_head_widths"] == over_budget_widths(pipeline)
    assert history["rollback_records"][0]["version"] == 2

    inconsistent = store.commit(
        pipeline,
        suggested,
        expected_base_version=2,
        window_name="compact",
        adjudication_digest="0" * 64,
        verify_full=True,
    )
    assert inconsistent["version"] == 3
    assert inconsistent["status"] == "rolled_back"
    assert "adjudication_digest_mismatch" in {
        item["code"] for item in inconsistent["validation"]["errors"]
    }
    assert inconsistent["effective_head_widths"] == suggested

    service = WorkbenchService(workspace)
    width_query = service.width_query("compact", query_id="rollback-query")
    assert width_query["version"] == 3
    assert width_query["version_status"] == "rolled_back"
    assert width_query["head_widths"] == suggested
    assert width_query["attempted_head_widths"] == suggested
    assert width_query["consistency"]["checked"] is True
    assert width_query["consistency"]["consistent"] is True
    assert width_query["consistency"]["full_run_verified_at_commit"] is True

    with pytest.raises(WorkbenchError) as raised:
        store.commit(
            pipeline,
            suggested,
            expected_base_version=1,
            window_name="compact",
            verify_full=False,
        )
    assert raised.value.code == "E_STALE_VERSION"


def test_version_rollback_detection(workspace: Path) -> None:
    store = VersionStore(workspace)
    pipeline = make_pipeline(workspace)
    suggested = pipeline.run("compact", mode="incremental")["payload"]["allocation"]["suggested_head_widths"]
    store.commit(pipeline, suggested, expected_base_version=0, window_name="compact", verify_full=False)
    old_head = (workspace / ".workbench" / "HEAD.json").read_bytes()
    store.commit(pipeline, suggested, expected_base_version=1, window_name="compact", verify_full=False)
    (workspace / ".workbench" / "HEAD.json").write_bytes(old_head)
    with pytest.raises(WorkbenchError) as raised:
        store.history()
    assert raised.value.code == "E_STALE_VERSION"
    assert raised.value.details["cause"] == "version_rollback"
    assert raised.value.details["head_version"] == 1
    assert raised.value.details["highest_version"] == 2


def test_record_tampering_and_fingerprint_change_detection(workspace: Path) -> None:
    store = VersionStore(workspace)
    pipeline = make_pipeline(workspace)
    suggested = pipeline.run("compact", mode="incremental")["payload"]["allocation"]["suggested_head_widths"]
    store.commit(pipeline, suggested, expected_base_version=0, window_name="compact", verify_full=False)
    record_path = workspace / ".workbench" / "versions" / "000000000001.json"
    record = read_json(record_path)
    record["head_widths"]["0"] = 12
    content = dict(record)
    content.pop("record_digest", None)
    record["record_digest"] = digest_object(content)
    write_json_file(record_path, record)
    with pytest.raises(WorkbenchError) as raised:
        store.current()
    assert raised.value.code == "E_STALE_VERSION"
    assert raised.value.details["cause"] == "record_tampering"


def test_manual_rollback_recover_and_temp_cleanup(workspace: Path) -> None:
    store = VersionStore(workspace)
    pipeline = make_pipeline(workspace)
    suggested = pipeline.run("compact", mode="incremental")["payload"]["allocation"]["suggested_head_widths"]
    store.commit(pipeline, suggested, expected_base_version=0, window_name="compact", verify_full=False)
    store.commit(pipeline, suggested, expected_base_version=1, window_name="compact", verify_full=False)
    manual = store.rollback_to(target_version=1, expected_base_version=2, note="manual restore for test")
    assert manual["version"] == 3
    assert manual["status"] == "manual_rollback"
    assert manual["effective_head_widths"] == suggested
    assert store.history()["current_version"] == 3

    temp_file = workspace / ".workbench" / "versions" / ".tmp-half-written"
    temp_file.write_bytes(b'{"half":')
    (workspace / ".workbench" / "HEAD.json").unlink()
    with pytest.raises(WorkbenchError) as raised:
        store.current()
    assert raised.value.code == "E_STALE_VERSION"
    assert raised.value.details["cause"] == "missing_head"
    recovered = store.recover()
    assert not temp_file.exists()
    assert recovered["recovered"] is True
    assert recovered["current_version"] == 3
    assert recovered["current_status"] == "manual_rollback"


def test_atomic_write_failure_leaves_no_half_record(workspace: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    target = workspace / ".workbench" / "versions" / "atomic-probe.json"

    def fail_replace(source: str | Path, destination: str | Path) -> None:
        raise OSError("injected interruption before replace")

    monkeypatch.setattr(canonical.os, "replace", fail_replace)
    with pytest.raises(OSError, match="injected interruption"):
        canonical.atomic_write_json(target, {"half": "record"})
    assert not target.exists()
    assert not list(target.parent.glob(".tmp-*"))


def _concurrent_commit_worker(arguments: tuple[str, dict[str, int]]) -> dict[str, Any]:
    root_string, widths = arguments
    root = Path(root_string)
    store = VersionStore(root)
    pipeline = make_pipeline(root)
    try:
        result = store.commit(
            pipeline,
            widths,
            expected_base_version=0,
            window_name="compact",
            verify_full=False,
        )
        return {"ok": True, "version": result["version"], "status": result["status"]}
    except WorkbenchError as exc:
        return {"ok": False, "code": exc.code, "details": exc.details}


def test_concurrent_file_lock_allows_one_contiguous_commit(workspace: Path) -> None:
    reset_versions(workspace)
    reset_stage_cache(workspace)
    pipeline = make_pipeline(workspace)
    suggested = pipeline.run("compact", mode="incremental")["payload"]["allocation"]["suggested_head_widths"]
    arguments = [(str(workspace), suggested) for _ in range(4)]
    context = multiprocessing.get_context("fork")
    with context.Pool(processes=4) as pool:
        results = pool.map(_concurrent_commit_worker, arguments)
    successes = [result for result in results if result["ok"]]
    failures = [result for result in results if not result["ok"]]
    assert len(successes) == 1
    assert successes[0]["version"] == 1
    assert len(failures) == 3
    assert all(result["code"] == "E_STALE_VERSION" for result in failures)
    assert all(result["details"]["cause"] == "concurrent_or_duplicate_commit" for result in failures)
    history = VersionStore(workspace).history()
    assert [item["version"] for item in history["versions"]] == [1]

