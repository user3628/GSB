"""阶段四：结果封装与真值验证。

* 导出确定性 ASCII PLY 点云（坐标固定有效位、按体素键排序）；
* 与清单真值点云做一对一最优匹配（总欧氏距离最小的贪心分配），
  计算 RMSE、最大误差、覆盖率与逐点误差，产出验证摘要。

真值点云只来自本地生成器清单，是验证的唯一真值来源。
"""

from __future__ import annotations

import os

from .canonical import atomic_write, round_float, write_json
from .errors import ManifestError

PACKAGING_STAGE = "packaging"


def write_ply(path: str, points: list[dict]) -> None:
    lines = [
        "ply",
        "format ascii 1.0",
        f"element vertex {len(points)}",
        "property float x",
        "property float y",
        "property float z",
        "property float reprojection_error",
        "end_header",
    ]
    for row in points:
        x, y, z = row["point"]
        lines.append(
            f"{round_float(x)} {round_float(y)} {round_float(z)} {round_float(row['max_reprojection_error'])}"
        )
    atomic_write(path, ("\n".join(lines) + "\n").encode("utf-8"))


def _greedy_match(estimated, truth):
    import math

    pairs = []
    for i, est in enumerate(estimated):
        for j, gt in enumerate(truth):
            distance = math.sqrt(sum((a - b) ** 2 for a, b in zip(est["point"], gt["world"])))
            pairs.append((distance, i, j))
    pairs.sort(key=lambda item: (item[0], item[1], item[2]))
    used_est, used_truth, matches = set(), set(), []
    for distance, i, j in pairs:
        if i in used_est or j in used_truth:
            continue
        used_est.add(i)
        used_truth.add(j)
        matches.append(
            {
                "track_id": estimated[i]["track_id"],
                "truth_id": truth[j]["point_id"],
                "distance": round_float(distance),
            }
        )
    return matches


def validate_and_package(geometry: dict, manifest: dict, export_dir: str) -> dict:
    truth = manifest.get("truth_points")
    if not truth:
        raise ManifestError("manifest has no truth_points", stage=PACKAGING_STAGE, tile_id="global")
    points = geometry["points"]
    matches = _greedy_match(points, truth)
    distances = [row["distance"] for row in matches]
    count = len(distances)
    rmse = round_float((sum(d * d for d in distances) / count) ** 0.5) if count else float("inf")
    per_point = sorted(matches, key=lambda row: row["truth_id"])
    summary = {
        "stage": PACKAGING_STAGE,
        "point_count": len(points),
        "truth_count": len(truth),
        "matched_count": count,
        "coverage": round_float(count / len(truth)),
        "rmse": rmse,
        "max_error": round_float(max(distances)) if distances else None,
        "mean_error": round_float(sum(distances) / count) if count else None,
        "per_point": per_point,
        "geometry_counts": geometry["counts"],
        "reprojection_threshold": manifest["reprojection_threshold"],
        "sample_density": manifest["sample_density"],
        "config_version": manifest["config_version"],
    }
    os.makedirs(export_dir, exist_ok=True)
    ply_path = os.path.join(export_dir, "point_cloud.ply")
    summary_path = os.path.join(export_dir, "validation_summary.json")
    write_ply(ply_path, points)
    write_json(summary_path, summary)
    return summary
