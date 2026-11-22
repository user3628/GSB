from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from density_trace.canonical import (
    atomic_write_json,
)
from density_trace.demo import rewrite_batch_for_invalidation
from density_trace.engine import Engine, import_inputs
from density_trace.errors import BusinessError

from .conftest import make_engine


def _import_engine(workspace: Path, data: Path) -> Engine:
    import_inputs(
        workspace,
        data / "manifest.json",
        data / "config.json",
        data / "reference_quantiles.json",
    )
    return Engine.from_workspace(workspace)


def test_drift_adjudication_and_reference_invalid(
    tmp_path: Path, data_copy: Path
) -> None:
    engine = make_engine(tmp_path / "workspace", data_copy)
    engine.resume()
    verdict = engine.get_verdict(0)
    assert verdict["decision"] in {"stable", "warning", "drift"}
    assert verdict["thresholds"]["warning"] < verdict["thresholds"]["drift"]
    assert verdict["confidence_levels"]["warning"] == 0.9
    assert verdict["confidence_levels"]["drift"] == 0.99
    assert verdict["quantile_index_start"] == 1
    assert verdict["quantile_index_end"] == 17
    assert verdict["state_version"] == 1
    assert verdict["quantile_count"] == 17
    assert verdict["score"] >= 0
    assert engine.get_verdict(1)["decision"] == "drift"

    missing_data = tmp_path / "missing-reference-data"
    shutil.copytree(data_copy, missing_data)
    reference = json.loads(
        (missing_data / "reference_quantiles.json").read_text(encoding="utf-8")
    )
    reference["quantiles"] = [
        point for point in reference["quantiles"] if point["index"] != 7
    ]
    atomic_write_json(missing_data / "reference_quantiles.json", reference)
    missing_engine = _import_engine(tmp_path / "missing-reference-ws", missing_data)
    with pytest.raises(BusinessError) as missing:
        missing_engine.run(batch_id=0)
    assert missing.value.code == "E_REFERENCE_INVALID"
    assert missing.value.details["batch_id"] == 0
    assert missing.value.details["quantile_index"] == 7
    assert not list(
        (tmp_path / "missing-reference-ws" / "checkpoints").glob("checkpoint-*.json")
    )

    broken_data = tmp_path / "broken-reference-data"
    shutil.copytree(data_copy, broken_data)
    reference = json.loads(
        (broken_data / "reference_quantiles.json").read_text(encoding="utf-8")
    )
    reference["quantiles"][7]["y_value"] = reference["quantiles"][6]["y_value"]
    atomic_write_json(broken_data / "reference_quantiles.json", reference)
    broken_engine = _import_engine(tmp_path / "broken-reference-ws", broken_data)
    with pytest.raises(BusinessError) as broken:
        broken_engine.run(batch_id=0)
    assert broken.value.code == "E_REFERENCE_INVALID"
    assert broken.value.details["batch_id"] == 0
    assert broken.value.details["quantile_index"] == 7


def test_graceful_stop_resume_is_byte_identical(
    tmp_path: Path, data_copy: Path
) -> None:
    continuous = make_engine(tmp_path / "continuous", data_copy)
    continuous.resume()
    continuous_summary = continuous.final_summary()

    resumed = make_engine(tmp_path / "resumed", data_copy)
    resumed.run(batch_id=0)
    resumed.run(batch_id=1)
    resumed.request_stop({"test": "between-batches"})
    stopped = resumed.resume()
    assert stopped["status"] == "stopped"
    assert stopped["published_batches"] == []
    recovered = resumed.resume()
    assert recovered["status"] == "complete"
    assert recovered["published_batches"] == [2, 3]
    assert resumed.final_summary() == continuous_summary
    for sequence in range(1, 5):
        assert resumed.workspace.record_bytes(
            sequence
        ) == continuous.workspace.record_bytes(sequence)
    for batch_id in range(4):
        assert resumed.get_verdict(batch_id) == continuous.get_verdict(batch_id)
        assert resumed.get_error_report(batch_id) == continuous.get_error_report(
            batch_id
        )
    history = resumed.workspace.history()
    assert [record["sequence"] for record in history.records] == [1, 2, 3, 4]
    assert [record["batch_id"] for record in history.records] == [0, 1, 2, 3]
    assert resumed.workspace.stop_ack_path.exists()


def test_interrupted_atomic_temp_file_is_ignored(
    tmp_path: Path, data_copy: Path
) -> None:
    engine = make_engine(tmp_path / "workspace", data_copy)
    engine.run(batch_id=0)
    temp = engine.workspace.checkpoints_dir / ".checkpoint-00000002.json.abcd.tmp"
    temp.write_bytes(b'{"half":')
    assert engine.workspace.history().next_sequence == 2
    engine.resume()
    assert engine.final_summary()["status"] == "complete"


def test_content_change_invalidates_earliest_batch_and_reuses_prefix(
    tmp_path: Path, data_copy: Path
) -> None:
    complete = make_engine(tmp_path / "original", data_copy)
    complete.resume()
    original_prefix = {
        sequence: complete.workspace.record_bytes(sequence) for sequence in (1, 2)
    }

    changed_data = tmp_path / "changed-data"
    shutil.copytree(data_copy, changed_data)
    changed_ws = tmp_path / "changed-workspace"
    shutil.copytree(complete.workspace.root, changed_ws)
    import_inputs(
        changed_ws,
        changed_data / "manifest.json",
        changed_data / "config.json",
        changed_data / "reference_quantiles.json",
    )
    rewrite_batch_for_invalidation(changed_data, batch_id=2)
    engine = Engine.from_workspace(changed_ws)
    plan = engine.inspect_versions()
    assert plan["requires_invalidation"] is True
    assert plan["earliest_affected_batch"] == 2
    assert plan["reused_batch_range"] == [0, 1]
    assert "batch-content:2" in plan["reasons"]
    with pytest.raises(BusinessError) as stale_query:
        engine.get_verdict(2)
    assert stale_query.value.code == "E_STALE_VERSION"

    reconciliation = engine.reconcile()
    event = reconciliation["event"]
    assert event["affected_batch_start"] == 2
    assert event["affected_batch_end"] == 3
    assert event["new_version_fingerprint"] == plan["new_version_fingerprint"]
    history = engine.workspace.history()
    assert [record["batch_id"] for record in history.records] == [0, 1]
    assert engine.workspace.record_bytes(1) == original_prefix[1]
    assert engine.workspace.record_bytes(2) == original_prefix[2]
    engine.resume()
    final = engine.final_summary()
    assert final["status"] == "complete"
    assert final["generation"] == 2
    version_info = engine.version_info()
    assert version_info["latest_invalidation"]["reason"] == "batch-content:2"
    assert version_info["latest_invalidation"]["affected_batch_start"] == 2
    assert (
        version_info["latest_invalidation"]["new_version_fingerprint"]
        == plan["new_version_fingerprint"]
    )


@pytest.mark.parametrize(
    ("section", "key", "value", "reason"),
    [
        ("density", "chunk_size", 17, "bandwidth/chunk/sum/grid-density-config"),
        (
            "adjudication",
            "warning_threshold",
            0.031,
            "drift-threshold/reference-quantiles",
        ),
    ],
)
def test_config_change_invalidates_from_zero(
    tmp_path: Path,
    data_copy: Path,
    section: str,
    key: str,
    value: float,
    reason: str,
) -> None:
    complete = make_engine(tmp_path / f"original-{key}", data_copy)
    complete.resume()
    config = json.loads((data_copy / "config.json").read_text(encoding="utf-8"))
    config[section][key] = value
    atomic_write_json(data_copy / "config.json", config)
    engine = Engine.from_workspace(complete.workspace.root)
    plan = engine.inspect_versions()
    assert plan["requires_invalidation"] is True
    assert plan["earliest_affected_batch"] == 0
    assert reason in plan["reasons"]
    reconciliation = engine.reconcile()
    assert reconciliation["event"]["affected_batch_start"] == 0
    assert engine.workspace.history().records == ()
