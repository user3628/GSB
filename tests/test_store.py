from __future__ import annotations

import multiprocessing
import shutil
from pathlib import Path
from queue import Empty
from typing import Any

import numpy as np
import pytest

from quant_audit.canonical import atomic_write_json, read_json
from quant_audit.errors import (
    E_AUDIT_TAMPERED,
    E_INVALID_ROLLBACK,
    E_INVALID_STAGE,
    E_STALE_VERSION,
    E_VERSION_ROLLBACK,
    AuditError,
)
from quant_audit.workspace import Workspace


def _publish_once(root: str, barrier: Any, queue: Any) -> None:
    try:
        workspace = Workspace.open(root)
        result = {
            "layers": [],
            "end_to_end": {},
            "attribution": {},
            "budget": {"usable": True, "over_budget_layers": []},
        }
        barrier.wait(timeout=10)
        published = workspace.store.publish_calibration(
            base_version=0,
            result=result,
            arrays={"probe": np.array([1.0, 2.0], dtype=np.float64)},
            inputs={"layer_names": []},
            layer_fingerprints={},
            budget_config_fingerprint="probe-budget",
            execution={"mode": "test"},
        )
        queue.put(("ok", published["record"]["version"]))
    except AuditError as exc:
        queue.put(("error", exc.code))
    except Exception as exc:  # noqa: BLE001  # pragma: no cover
        queue.put(("exception", f"{type(exc).__name__}: {exc}"))


def test_history_is_monotonic_and_contiguous(demo_paths: dict[str, Path]) -> None:
    workspace = demo_paths["workspace"]
    v1 = workspace.calibrate(mode="full")
    v2 = workspace.calibrate(mode="incremental", config_path=demo_paths["data"] / "config_incremental.yaml")
    history = workspace.history()

    assert v1["published_version"] == 1
    assert v2["published_version"] == 2
    assert [record["version"] for record in history["records"]] == [1, 2]
    assert history["active_version"] == 2


def test_stale_and_duplicate_publication_returns_stale(demo_paths: dict[str, Path]) -> None:
    workspace = demo_paths["workspace"]
    workspace.calibrate(mode="full")
    with pytest.raises(AuditError) as caught:
        workspace.store.publish_calibration(
            base_version=0,
            result={},
            arrays={},
            inputs={},
            layer_fingerprints={},
            budget_config_fingerprint="x",
            execution={},
        )
    assert caught.value.code == E_STALE_VERSION


def test_stale_explicit_incremental_base_returns_stale(demo_paths: dict[str, Path]) -> None:
    workspace = demo_paths["workspace"]
    workspace.calibrate(mode="full")
    workspace.calibrate(mode="incremental", config_path=demo_paths["data"] / "config_incremental.yaml")
    with pytest.raises(AuditError) as caught:
        workspace.calibrate(
            mode="incremental",
            base_version=1,
            config_path=demo_paths["data"] / "config_incremental.yaml",
        )
    assert caught.value.code == E_STALE_VERSION
    assert caught.value.details["active_version"] == 2


def test_missing_stage_returns_invalid_stage(demo_paths: dict[str, Path]) -> None:
    workspace = demo_paths["workspace"]
    workspace.calibrate(mode="full")
    (workspace.audit_root / "stages" / "00000001.npz").unlink()
    with pytest.raises(AuditError) as caught:
        workspace.store.load_stage(1)
    assert caught.value.code == E_INVALID_STAGE


def test_budget_failure_rolls_back_and_matches_history(demo_paths: dict[str, Path]) -> None:
    workspace = demo_paths["workspace"]
    workspace.calibrate(mode="full")
    over = workspace.calibrate(
        mode="full",
        config_path=demo_paths["data"] / "config_outlier_over_budget.yaml",
        calibration_path=demo_paths["data"] / "calibration_outlier.npy",
    )
    assert over["rolled_back"] is True
    assert over["published_version"] == 2
    assert workspace.store.active_version() == 1
    queried = workspace.query()
    historical = workspace.store.load_record(1)
    assert queried["result"] == historical["result"]
    manual = workspace.rollback()
    assert manual["active_version"] == 1
    assert workspace.query()["result_summary"] == historical["result_summary"]
    with pytest.raises(AuditError) as caught:
        workspace.rollback(2)
    assert caught.value.code == E_INVALID_ROLLBACK


def test_head_rollback_is_classified_as_version_rollback(demo_paths: dict[str, Path]) -> None:
    workspace = demo_paths["workspace"]
    workspace.calibrate(mode="full")
    old_head = read_json(workspace.audit_root / "HEAD.json")
    workspace.calibrate(mode="incremental", config_path=demo_paths["data"] / "config_incremental.yaml")

    atomic_write_json(workspace.audit_root / "HEAD.json", old_head)
    with pytest.raises(AuditError) as rollback_caught:
        workspace.query()
    assert rollback_caught.value.code == E_VERSION_ROLLBACK


def test_whole_history_replacement_is_rejected_by_anchor(demo_paths: dict[str, Path]) -> None:
    workspace = demo_paths["workspace"]
    workspace.calibrate(mode="full")
    snapshot = demo_paths["root"] / "v1_audit_snapshot"
    shutil.copytree(workspace.audit_root, snapshot)
    workspace.calibrate(mode="incremental", config_path=demo_paths["data"] / "config_incremental.yaml")
    shutil.rmtree(workspace.audit_root)
    shutil.copytree(snapshot, workspace.audit_root)

    with pytest.raises(AuditError) as caught:
        workspace.query()
    assert caught.value.code == E_VERSION_ROLLBACK


def test_record_summary_and_parameter_fingerprint_tamper(tmp_path: Path, demo_paths: dict[str, Path]) -> None:
    for field in ("result_summary", "parameter_fingerprint"):
        root = tmp_path / field
        shutil.copytree(demo_paths["workspace"].root, root)
        workspace = Workspace.open(root)
        workspace.calibrate(mode="full")
        record_path = workspace.audit_root / "records" / "00000001.json"
        record = read_json(record_path)
        if field == "result_summary":
            record["result_summary"] = "0" * 64
        else:
            record["layer_fingerprints"]["linear_00"] = "f" * 64
        atomic_write_json(record_path, record)
        with pytest.raises(AuditError) as caught:
            workspace.query()
        assert caught.value.code == E_AUDIT_TAMPERED


def test_interrupted_record_write_leaves_no_half_record(
    demo_paths: dict[str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = demo_paths["workspace"]
    workspace.calibrate(mode="full")
    import quant_audit.store as store_module

    original = store_module.atomic_write_json

    def failing(path: Path, value: Any) -> None:
        if Path(path).name == "00000002.json":
            raise OSError("injected interruption")
        original(path, value)

    monkeypatch.setattr(store_module, "atomic_write_json", failing)
    with pytest.raises(OSError):
        workspace.calibrate(mode="incremental", config_path=demo_paths["data"] / "config_incremental.yaml")
    monkeypatch.setattr(store_module, "atomic_write_json", original)

    assert not (workspace.audit_root / "records" / "00000002.json").exists()
    assert workspace.store.latest_version() == 1
    assert workspace.query()["version"] == 1


def test_interrupted_head_write_recovers_transaction(
    demo_paths: dict[str, Path],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workspace = demo_paths["workspace"]
    workspace.calibrate(mode="full")
    import quant_audit.store as store_module

    original = store_module.atomic_write_json

    def failing(path: Path, value: Any) -> None:
        if Path(path).name == "HEAD.json":
            raise OSError("injected interruption")
        original(path, value)

    monkeypatch.setattr(store_module, "atomic_write_json", failing)
    with pytest.raises(OSError):
        workspace.calibrate(mode="incremental", config_path=demo_paths["data"] / "config_incremental.yaml")
    monkeypatch.setattr(store_module, "atomic_write_json", original)

    assert workspace.store.latest_version() == 2
    assert workspace.query()["version"] == 2


def test_concurrent_publishers_allow_only_one_next_version(demo_paths: dict[str, Path]) -> None:
    workspace = demo_paths["workspace"]
    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    queue = context.Queue()
    processes = [
        context.Process(target=_publish_once, args=(str(workspace.root), barrier, queue))
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    outcomes = []
    try:
        for _ in processes:
            outcomes.append(queue.get(timeout=20))
    except Empty as exc:
        raise AssertionError("concurrent publishers did not report within timeout") from exc
    finally:
        for process in processes:
            process.join(timeout=20)
            if process.is_alive():
                process.terminate()

    assert sorted(outcome[0] for outcome in outcomes) == ["error", "ok"]
    assert [outcome[1] for outcome in outcomes if outcome[0] == "error"] == [E_STALE_VERSION]
    assert workspace.store.latest_version() == 1

