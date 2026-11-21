from __future__ import annotations

import json
import shutil
from pathlib import Path

from PIL import Image

from gridpipe.canonical import dump_bytes
from gridpipe.pipeline import Pipeline


def _export_bytes(pipeline: Pipeline) -> bytes:
    return dump_bytes(pipeline.export_data())


def test_parallel_and_serial_exports_are_byte_identical(
    sample_project: Path, manifest_path: Path
) -> None:
    parallel = Pipeline(
        sample_project,
        manifest_path,
        work_dir=sample_project / "work" / "parallel",
        output_dir=sample_project / "output" / "parallel",
    )
    serial = Pipeline(
        sample_project,
        manifest_path,
        work_dir=sample_project / "work" / "serial",
        output_dir=sample_project / "output" / "serial",
    )
    parallel_result = parallel.run(execution_workers=2)
    serial_result = serial.run(execution_workers=1)
    assert parallel_result["status"] == "completed"
    assert serial_result["status"] == "completed"
    assert _export_bytes(parallel) == _export_bytes(serial)
    summary = parallel.export_data()["summary"]
    assert summary["table_region_iou_mean"] == 1.0
    assert summary["chart_region_iou_mean"] == 1.0
    assert summary["cell_row_col_accuracy_mean"] == 1.0
    assert summary["chart_series_mae_mean"] < 0.5
    assert summary["error_count"] == 0


def test_budget_rejects_shard_and_never_exceeds_total(
    sample_project: Path, manifest_path: Path
) -> None:
    pipeline = Pipeline(
        sample_project,
        manifest_path,
        work_dir=sample_project / "work" / "budget",
        output_dir=sample_project / "output" / "budget",
    )
    budget = pipeline.budget_report()
    assert budget["within_total_budget"] is True
    assert budget["reserved_pixels"] <= budget["total_pixel_budget"]
    assert all(item["estimated_pixels"] <= budget["worker_pixel_quota"] for item in budget["accepted"])
    assert budget["unprocessed"]
    assert budget["unprocessed"][0]["shard_id"] == "shard-0005"
    assert budget["unprocessed"][0]["reason"] == "total_pixel_budget_exceeded"
    result = pipeline.run(execution_workers=1)
    assert result["status"] == "completed"
    assert result["accepted_shard_ids"] == ["shard-0001", "shard-0002", "shard-0003", "shard-0004"]


def test_stop_resume_matches_uninterrupted_run(
    sample_project: Path, manifest_path: Path
) -> None:
    full = Pipeline(
        sample_project,
        manifest_path,
        work_dir=sample_project / "work" / "full-stop-compare",
        output_dir=sample_project / "output" / "full-stop-compare",
    )
    full_result = full.run(execution_workers=2)
    assert full_result["status"] == "completed"

    interrupted_work = sample_project / "work" / "interrupted"
    stopped = Pipeline(
        sample_project,
        manifest_path,
        work_dir=interrupted_work,
        output_dir=sample_project / "output" / "interrupted",
    )
    stopped_result = stopped.run(stop_after=2, execution_workers=2)
    assert stopped_result["status"] == "stopped"
    assert stopped_result["checkpoint_sequence"] == 2
    resumed = Pipeline(
        sample_project,
        manifest_path,
        work_dir=interrupted_work,
        output_dir=sample_project / "output" / "interrupted",
    )
    resumed_result = resumed.run(resume=True, execution_workers=2)
    assert resumed_result["status"] == "completed"
    assert _export_bytes(full) == _export_bytes(resumed)


def test_incremental_invalidation_starts_at_table(
    sample_project: Path, manifest_path: Path
) -> None:
    work = sample_project / "work" / "invalidation"
    first = Pipeline(sample_project, manifest_path, work_dir=work, output_dir=sample_project / "output" / "inv")
    assert first.run(execution_workers=1)["status"] == "completed"

    changed = Pipeline(
        sample_project,
        manifest_path,
        work_dir=work,
        output_dir=sample_project / "output" / "inv",
        config_overrides={"table": {"segment_coverage": 0.46}},
    )
    assert all(item["earliest_stage"] == "table" for item in changed.invalidation)
    result = changed.run(execution_workers=1)
    assert result["status"] == "completed"
    assert result["stats"]["cache_reused"]["detection"] == 4
    assert result["stats"]["computed"]["detection"] == 0
    assert result["stats"]["computed"]["table"] == 4
    assert result["stats"]["computed"]["chart"] == 4


def test_invalidation_mapping_for_detection_chart_and_budget(
    sample_project: Path, manifest_path: Path
) -> None:
    work = sample_project / "work" / "fingerprint-families"
    output = sample_project / "output" / "fingerprint-families"
    base = Pipeline(sample_project, manifest_path, work_dir=work, output_dir=output)
    assert base.run(execution_workers=1)["status"] == "completed"

    detection_changed = Pipeline(
        sample_project,
        manifest_path,
        work_dir=work,
        output_dir=output,
        config_overrides={"detection": {"threshold": 61}},
    )
    assert all(item["earliest_stage"] == "detection" for item in detection_changed.invalidation)
    detection_result = detection_changed.run(execution_workers=1)
    assert detection_result["stats"]["computed"]["detection"] == 4
    assert detection_result["stats"]["cache_reused"]["detection"] == 0

    Pipeline(sample_project, manifest_path, work_dir=work, output_dir=output).run(execution_workers=1)
    chart_changed = Pipeline(
        sample_project,
        manifest_path,
        work_dir=work,
        output_dir=output,
        config_overrides={"chart": {"color_channel_delta": 29}},
    )
    assert all(item["earliest_stage"] == "chart" for item in chart_changed.invalidation)
    chart_result = chart_changed.run(execution_workers=1)
    assert chart_result["stats"]["computed"]["chart"] == 4
    assert chart_result["stats"]["cache_reused"]["detection"] == 4
    assert chart_result["stats"]["cache_reused"]["table"] == 4

    Pipeline(sample_project, manifest_path, work_dir=work, output_dir=output).run(execution_workers=1)
    budget_changed = Pipeline(
        sample_project,
        manifest_path,
        work_dir=work,
        output_dir=output,
        config_overrides={"budget": {"total_pixels": 1349000}},
    )
    assert all(item["earliest_stage"] == "validation" for item in budget_changed.invalidation)
    budget_result = budget_changed.run(execution_workers=1)
    assert budget_result["stats"]["computed"]["validation"] == 4
    assert budget_result["stats"]["cache_reused"]["detection"] == 4
    assert budget_result["stats"]["cache_reused"]["table"] == 4
    assert budget_result["stats"]["cache_reused"]["chart"] == 4


def test_pixel_content_change_starts_invalidation_at_detection(
    tmp_path: Path, sample_project: Path
) -> None:
    root = tmp_path / "pixel-project"
    shutil.copytree(sample_project / "data", root / "data")
    manifest = root / "data" / "manifest.json"
    work = root / "work"
    first = Pipeline(root, manifest, work_dir=work, output_dir=root / "output")
    assert first.run(execution_workers=1)["status"] == "completed"

    image_path = root / "data" / "images" / "shard-0001.png"
    with Image.open(image_path) as opened:
        image = opened.convert("RGB")
    image.putpixel((1, 1), (250, 249, 248))
    image.save(image_path)

    changed = Pipeline(root, manifest, work_dir=work, output_dir=root / "output")
    shard_report = next(item for item in changed.invalidation if item["shard_id"] == "shard-0001")
    assert shard_report["earliest_stage"] == "detection"
    result = changed.run(execution_workers=1)
    assert result["status"] == "completed"
    assert result["stats"]["computed"]["detection"] == 1
    assert result["stats"]["cache_reused"]["detection"] == 3


def test_explicit_stop_request_then_resume(sample_project: Path, manifest_path: Path) -> None:
    work = sample_project / "work" / "stop-request"
    pipeline = Pipeline(sample_project, manifest_path, work_dir=work, output_dir=sample_project / "output" / "stop-request")
    pipeline.request_stop()
    stopped = pipeline.run(execution_workers=1)
    assert stopped["status"] == "stopped"
    assert stopped["stop_reason"] == "stop_request"
    assert stopped["checkpoint_sequence"] == 0
    resumed = pipeline.run(resume=True, execution_workers=1)
    assert resumed["status"] == "completed"
