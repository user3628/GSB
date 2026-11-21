from __future__ import annotations

import copy
import json
from pathlib import Path

import numpy as np
import pytest
from PIL import Image, ImageDraw

from gridpipe.chart_structure import extract_chart
from gridpipe.checkpoints import CheckpointStore
from gridpipe.config import default_configs
from gridpipe.detection import detect_regions
from gridpipe.errors import (
    E_CHART_AXIS,
    E_INVALID_REGION,
    E_INVALID_STAGE,
    E_STALE_CHECKPOINT,
    E_TABLE_STRUCTURE,
    E_WORKER,
    PipelineError,
)
from gridpipe.imaging import load_rgb_and_gray
from gridpipe.pipeline import Pipeline, execute_stage_task
from gridpipe.table_structure import restore_table


def _first_shard(manifest_path: Path):
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    return manifest, manifest["shards"][0]


def test_invalid_region_code() -> None:
    blank = np.full((120, 160), 255, dtype=np.uint8)
    with pytest.raises(PipelineError) as caught:
        detect_regions(blank, shard_id="probe", config=default_configs()["detection"])
    assert caught.value.code == E_INVALID_REGION
    assert caught.value.shard_id == "probe"
    assert caught.value.location == [0, 0, 160, 120]


def test_table_structure_code(manifest_path: Path) -> None:
    _, shard = _first_shard(manifest_path)
    rgb, gray = load_rgb_and_gray(manifest_path.parent / shard["image"])
    configs = default_configs()
    detection = detect_regions(gray, shard_id=shard["id"], config=configs["detection"])
    declared = copy.deepcopy(shard["truth"]["table"])
    declared["rows"] += 1
    with pytest.raises(PipelineError) as caught:
        restore_table(
            rgb,
            gray,
            region=detection["table"]["pixel_rect"],
            declared=declared,
            config=configs["table"],
            shard_id=shard["id"],
        )
    assert caught.value.code == E_TABLE_STRUCTURE
    assert caught.value.to_dict()["diagnostics"]["reason"] == "row_column_count_mismatch"


def test_chart_axis_code_preserves_partial_series(
    manifest_path: Path, tmp_path: Path
) -> None:
    _, shard = _first_shard(manifest_path)
    image_path = manifest_path.parent / shard["image"]
    rgb, gray = load_rgb_and_gray(image_path)
    configs = default_configs()
    detection = detect_regions(gray, shard_id=shard["id"], config=configs["detection"])
    chart_region = detection["chart"]["pixel_rect"]
    chart = extract_chart(
        rgb,
        gray,
        region=chart_region,
        config=configs["chart"],
        shard_id=shard["id"],
    )
    tick_y = chart_region[1] + chart["ticks"]["y"][1]["pixel"]
    with Image.open(image_path) as opened:
        image = opened.convert("RGB")
    draw = ImageDraw.Draw(image)
    draw.rectangle(
        [chart_region[0] + 3, tick_y - 9, chart["plot_rect_pixel"][0] - 8, tick_y + 10],
        fill=(248, 249, 250),
    )
    damaged = tmp_path / "axis-damaged.png"
    image.save(damaged)
    damaged_rgb, damaged_gray = load_rgb_and_gray(damaged)
    with pytest.raises(PipelineError) as caught:
        extract_chart(
            damaged_rgb,
            damaged_gray,
            region=chart_region,
            config=configs["chart"],
            shard_id=shard["id"],
        )
    assert caught.value.code == E_CHART_AXIS
    assert caught.value.partial["series"]
    assert caught.value.partial["series"][0]["pixel_points"]


def test_stale_checkpoint_codes(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "work", "probe-run")
    with pytest.raises(PipelineError) as caught:
        store.publish(
            shard_id="shard-0001",
            shard_index=1,
            stage="detection",
            input_fingerprint="fp-1",
            config_version="cfg",
            artifact_bytes=b'{"probe":1}\n',
            sequence=2,
        )
    assert caught.value.code == E_STALE_CHECKPOINT

    store.publish(
        shard_id="shard-0001",
        shard_index=1,
        stage="detection",
        input_fingerprint="fp-1",
        config_version="cfg",
        artifact_bytes=b'{"probe":1}\n',
    )
    with pytest.raises(PipelineError) as duplicate:
        store.publish(
            shard_id="shard-0001",
            shard_index=1,
            stage="detection",
            input_fingerprint="fp-1",
            config_version="cfg",
            artifact_bytes=b'{"probe":1}\n',
        )
    assert duplicate.value.code == E_STALE_CHECKPOINT

    out_of_order_store = CheckpointStore(tmp_path / "work-cross-shard", "probe-run")
    out_of_order_store.publish(
        shard_id="shard-0002",
        shard_index=2,
        stage="detection",
        input_fingerprint="fp-2",
        config_version="cfg",
        artifact_bytes=b'{"probe":2}\n',
    )
    with pytest.raises(PipelineError) as cross_shard:
        out_of_order_store.publish(
            shard_id="shard-0001",
            shard_index=1,
            stage="detection",
            input_fingerprint="fp-1",
            config_version="cfg",
            artifact_bytes=b'{"probe":1}\n',
        )
    assert cross_shard.value.code == E_STALE_CHECKPOINT


def test_checkpoint_stage_jump_code(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "work", "probe-run")
    with pytest.raises(PipelineError) as caught:
        store.publish(
            shard_id="shard-0001",
            shard_index=1,
            stage="table",
            input_fingerprint="fp-table",
            config_version="cfg",
            artifact_bytes=b'{"probe":1}\n',
        )
    assert caught.value.code == E_INVALID_STAGE


def test_invalid_stage_request(sample_project: Path, manifest_path: Path) -> None:
    pipeline = Pipeline(
        sample_project,
        manifest_path,
        work_dir=sample_project / "work" / "illegal-stage",
        output_dir=sample_project / "output" / "illegal-stage",
    )
    with pytest.raises(PipelineError) as caught:
        pipeline.run(stage="chart", execution_workers=1)
    assert caught.value.code == E_INVALID_STAGE


def test_worker_exception_is_structured(monkeypatch: pytest.MonkeyPatch) -> None:
    import gridpipe.pipeline as pipeline_module

    def boom(*args, **kwargs):
        raise RuntimeError("synthetic worker failure")

    monkeypatch.setattr(pipeline_module, "detect_regions", boom)
    monkeypatch.setattr(
        pipeline_module,
        "load_rgb_and_gray",
        lambda path: (
            np.zeros((20, 20, 3), dtype=np.uint8),
            np.zeros((20, 20), dtype=np.uint8),
        ),
    )
    payload = execute_stage_task(
        {
            "stage": "detection",
            "shard": {"id": "shard-probe", "index": 99},
            "image_path": "/not/used.png",
            "config": default_configs()["detection"],
            "input_fingerprint": "probe",
            "prerequisites": {},
        }
    )
    assert payload["ok"] is False
    assert payload["error"]["code"] == E_WORKER
    assert payload["error"]["diagnostics"]["exception_type"] == "RuntimeError"
