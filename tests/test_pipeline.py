import json
import os
import shutil

import pytest

from iqa.pipeline import Pipeline
from iqa.config import Config
from iqa.injection import Injection
from iqa.errors import E_MISSING_REFERENCE, E_DIM_MISMATCH, E_NUMERIC, E_INVALID_CONFIG
from iqa.hashing import canonical


def _summary(work_dir):
    with open(os.path.join(work_dir, "outputs", "summary.json"), "rb") as handle:
        return handle.read()


def test_full_run_reproducible_and_codes(work_dir):
    pipeline = Pipeline(work_dir)
    summary = pipeline.run()
    codes = [item["code"] for item in summary["diagnostics"]]
    assert E_DIM_MISMATCH in codes
    assert len(summary["out_of_range"]) >= 1
    first = _summary(work_dir)
    Pipeline(work_dir).run()
    assert _summary(work_dir) == first


def test_stop_resume_matches_full_run(work_dir):
    full = Pipeline(work_dir)
    full.run()
    full_summary = _summary(work_dir)

    fresh = os.path.join(os.path.dirname(work_dir), "fresh")
    shutil.copytree(work_dir, fresh)
    for name in ("checkpoints", "state", "outputs"):
        shutil.rmtree(os.path.join(fresh, name), ignore_errors=True)

    pipeline = Pipeline(fresh)
    pipeline.store.request_stop()
    stopped = pipeline.run()
    assert stopped["stopped"] is True
    assert stopped["max_sequence"] >= 1

    resumed_pipeline = Pipeline(fresh)
    resumed_pipeline.store.clear_stop()
    resumed = resumed_pipeline.run()
    assert resumed["stopped"] is False
    assert resumed["max_sequence"] == 20
    assert _summary(fresh) == full_summary


def test_injection_codes(work_dir):
    for kind, code in [
        ("missing_reference", E_MISSING_REFERENCE),
        ("nan_pixels", E_NUMERIC),
        ("negative_weight", E_INVALID_CONFIG),
        ("dimension_mismatch", E_DIM_MISMATCH),
    ]:
        pipeline = Pipeline(work_dir, Config(), Injection(kind=kind, image_id="img_001_blocks"))
        summary = pipeline.run()
        assert any(item["code"] == code for item in summary["diagnostics"]), kind


def test_incremental_invalidation_on_weight_change(work_dir):
    Pipeline(work_dir).run()
    new_config = Config(weights={"sharpness": 0.4, "color": 0.2, "structure": 0.1, "semantic": 0.3})
    pipeline = Pipeline(work_dir, new_config)
    summary = pipeline.run()
    for result in summary["results"]:
        if result["final"] is not None:
            assert result["stage_status"]["stage4_fusion"] == "rebuilt"
            assert result["stage_status"]["stage1_normalize"] == "reused"


def test_incremental_invalidation_on_image_change(work_dir):
    Pipeline(work_dir).run()
    import numpy as np
    from PIL import Image

    image_path = os.path.join(work_dir, "images", "img_noise.png")
    with Image.open(image_path) as image:
        pixels = np.asarray(image.convert("RGB")).copy()
    pixels[0, 0, 0] = (int(pixels[0, 0, 0]) + 17) % 256
    Image.fromarray(pixels, mode="RGB").save(image_path, format="PNG", optimize=True)
    summary = Pipeline(work_dir).run()
    target = next(item for item in summary["results"] if item["image_id"] == "img_004_noise")
    assert target["stage_status"]["stage1_normalize"] == "rebuilt"
    assert target["stage_status"]["stage4_fusion"] == "rebuilt"
