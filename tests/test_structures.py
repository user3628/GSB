from __future__ import annotations

import json
from pathlib import Path

from gridpipe.chart_structure import extract_chart
from gridpipe.config import default_configs
from gridpipe.detection import detect_regions
from gridpipe.imaging import iou, load_rgb_and_gray
from gridpipe.table_structure import restore_table


def test_detection_and_structures_match_truth(manifest_path: Path) -> None:
    configs = default_configs()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    for shard in manifest["shards"][:4]:
        image_path = manifest_path.parent / shard["image"]
        rgb, gray = load_rgb_and_gray(image_path)
        detection = detect_regions(gray, shard_id=shard["id"], config=configs["detection"])
        assert iou(detection["table"]["region"], shard["truth"]["table"]["region"]) == 1.0
        assert iou(detection["chart"]["region"], shard["truth"]["chart"]["region"]) == 1.0

        table = restore_table(
            rgb,
            gray,
            region=detection["table"]["pixel_rect"],
            declared=shard["truth"]["table"],
            config=configs["table"],
            shard_id=shard["id"],
        )
        predicted = {
            (cell["row"], cell["col"], cell["row_span"], cell["col_span"])
            for cell in table["cells"]
        }
        expected = {
            (cell["row"], cell["col"], cell["row_span"], cell["col_span"])
            for cell in shard["truth"]["table"]["cells"]
        }
        assert predicted == expected
        assert table["header_rows"] == shard["truth"]["table"]["header_rows"]

        chart = extract_chart(
            rgb,
            gray,
            region=detection["chart"]["pixel_rect"],
            config=configs["chart"],
            shard_id=shard["id"],
        )
        assert chart["x_range"] == shard["truth"]["chart"]["x_range"]
        assert chart["y_range"] == shard["truth"]["chart"]["y_range"]
        truth_values = shard["truth"]["chart"]["series"][0]["values"]
        predicted_values = chart["series"][0]["values"]
        assert len(predicted_values) == len(truth_values)
        assert max(abs(a - b) for a, b in zip(predicted_values, truth_values)) < 0.5
