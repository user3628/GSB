import os

import numpy as np
import pytest

from iqa import normalize as stage1
from iqa.errors import BusinessError, E_DIM_MISMATCH, E_NUMERIC
from iqa.injection import Injection


def _meta(work_dir):
    import json

    metadata = json.load(open(os.path.join(work_dir, "metadata.json"), encoding="utf-8"))
    return metadata[0]


def test_dimension_mismatch_code_and_context(work_dir):
    metadata = _meta(work_dir)
    metadata["declared_width"] += 5
    with pytest.raises(BusinessError) as exc:
        stage1.run(os.path.join(work_dir, "images", metadata["file"]), metadata)
    assert exc.value.code == E_DIM_MISMATCH
    assert exc.value.context["image_id"] == metadata["image_id"]
    assert "declared" in exc.value.context and "actual" in exc.value.context


def test_nan_injection_numeric_code(work_dir):
    metadata = _meta(work_dir)
    injection = Injection(kind="nan_pixels", image_id=metadata["image_id"])
    with pytest.raises(BusinessError) as exc:
        stage1.run(os.path.join(work_dir, "images", metadata["file"]), metadata, injection)
    assert exc.value.code == E_NUMERIC
    assert exc.value.context["bad_pixels"] >= 1


def test_black_image_flagged(work_dir):
    import json

    metadata = next(item for item in json.load(open(os.path.join(work_dir, "metadata.json"), encoding="utf-8"))
                    if item["image_id"] == "img_005_black")
    record, pixels = stage1.run(os.path.join(work_dir, "images", metadata["file"]), metadata)
    assert record["product"]["is_all_black"] is True
    assert record["product"]["channels"] == 3
    assert record["product"]["valid_pixels"] == metadata["declared_width"] * metadata["declared_height"]


def test_summary_byte_stable(work_dir):
    metadata = _meta(work_dir)
    path = os.path.join(work_dir, "images", metadata["file"])
    record_a, _ = stage1.run(path, metadata)
    record_b, _ = stage1.run(path, metadata)
    assert stage1.canonical_summary(record_a) == stage1.canonical_summary(record_b)
    assert record_a["fingerprint"] == record_b["fingerprint"]
