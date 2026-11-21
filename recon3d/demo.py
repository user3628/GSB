"""本地确定性演示：一条命令跑通全部业务能力并打印固定摘要。

内容：
1. 固定种子生成 5 个分片（>=3）与 8 个真值点（>=6）；
2. 全量四阶段重建，输出固定验证摘要（RMSE/覆盖率/点数）；
3. 注入非正交位姿与非正焦距，验证 E_INVALID_POSE；
4. 阶段间停止后恢复，证明与从零全量逐字节一致；
5. 修改像素与误差阈值做增量重建，证明与全量逐字节一致（否则 E_INCREMENTAL_MISMATCH）；
6. 查询延迟预算 E_LATENCY_BUDGET 与缓存命中演示。
"""

from __future__ import annotations

import os
import shutil

from .canonical import dumps, read_json, read_png, write_json, write_png
from .errors import InvalidPoseError, LatencyBudgetError, StoppedError
from .generator import generate_samples
from .pipeline import Pipeline
from .query import QueryService


def _line(title: str) -> None:
    print("\n=== " + title + " ===")


def run_demo(work_dir: str = "demo_workspace") -> dict:
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir)
    sample_dir = os.path.join(work_dir, "samples")
    manifest = generate_samples(sample_dir)
    print(f"生成样本：{len(manifest['tiles'])} 个分片，{len(manifest['truth_points'])} 个真值点")
    for tile in manifest["tiles"]:
        print(f"  {tile['tile_id']}: {tile['width']}x{tile['height']}，可见 {len(tile['visible_points'])} 点")

    _line("1. 全量四阶段重建（固定摘要）")
    full_dir = os.path.join(work_dir, "full")
    full = Pipeline(sample_dir, full_dir)
    full.run(force=True)
    summary = read_json(os.path.join(full_dir, "exports", "validation_summary.json"))
    fixed_summary = {
        "point_count": summary["point_count"],
        "truth_count": summary["truth_count"],
        "coverage": summary["coverage"],
        "rmse": summary["rmse"],
        "max_error": summary["max_error"],
        "sample_fingerprint": full.sample_fp,
    }
    print(dumps(fixed_summary).decode())

    _line("2. 错误码注入：E_INVALID_POSE")
    bad_pose_dir = os.path.join(work_dir, "bad_pose")
    shutil.copytree(sample_dir, bad_pose_dir)
    bad_manifest = read_json(os.path.join(bad_pose_dir, "manifest.json"))
    bad_manifest["tiles"][0]["rotation"][0][0] += 0.5
    write_json(os.path.join(bad_pose_dir, "manifest.json"), bad_manifest)
    try:
        Pipeline(bad_pose_dir, os.path.join(work_dir, "run_bad_pose")).run(force=True)
    except InvalidPoseError as error:
        print("非正交旋转 ->", error.code, "tile:", error.payload["tile_id"])

    bad_focal_dir = os.path.join(work_dir, "bad_focal")
    shutil.copytree(sample_dir, bad_focal_dir)
    focal_manifest = read_json(os.path.join(bad_focal_dir, "manifest.json"))
    focal_manifest["tiles"][1]["intrinsics"][0][0] = 0.0
    write_json(os.path.join(bad_focal_dir, "manifest.json"), focal_manifest)
    try:
        Pipeline(bad_focal_dir, os.path.join(work_dir, "run_bad_focal")).run(force=True)
    except InvalidPoseError as error:
        print("零焦距 ->", error.code, "tile:", error.payload["tile_id"])

    _line("3. 阶段间停止 + 恢复一致性")
    stop_dir = os.path.join(work_dir, "stop_resume")
    try:
        Pipeline(sample_dir, stop_dir).run(force=True, stop_after="triangulation:global")
    except StoppedError as error:
        print("已停止于:", error.payload["stage"], error.payload["tile_id"])
    Pipeline(sample_dir, stop_dir).run(force=False)
    stop_pipe = Pipeline(sample_dir, stop_dir)
    identical = True
    for stage, scopes in [
        ("features", [t["tile_id"] for t in manifest["tiles"]]),
        ("triangulation", ["global"]),
        ("geometry", ["global"]),
        ("packaging", ["global"]),
    ]:
        for scope in scopes:
            if dumps(stop_pipe.store.read_artifact(stage, scope)) != dumps(
                full.store.read_artifact(stage, scope)
            ):
                identical = False
    print("停止/恢复与全量重建逐字节一致:", identical)

    _line("4. 增量重建一致性")
    changed_dir = os.path.join(work_dir, "changed")
    shutil.copytree(sample_dir, changed_dir)
    image_path = os.path.join(changed_dir, "images", "tile_02.png")
    image = read_png(image_path)
    image[2:6, 2:6] = 200
    write_png(image_path, image)
    changed_manifest = read_json(os.path.join(changed_dir, "manifest.json"))
    changed_manifest["reprojection_threshold"] = 1.1
    write_json(os.path.join(changed_dir, "manifest.json"), changed_manifest)
    inc_dir = os.path.join(work_dir, "incremental")
    Pipeline(sample_dir, inc_dir).run(force=True)
    incremental = Pipeline(changed_dir, inc_dir)
    result = incremental.incremental_rebuild(changed_manifest, changed_dir, inc_dir)
    print("增量与全量重建差异点数量:", result["mismatches"])

    _line("5. 查询延迟预算与缓存")
    service = QueryService(full_dir, full.sample_fp, manifest["config_version"], default_budget=1.0)
    service.stage_digests = {r["stage"]: r["artifact_digest"] for r in full.store.stage_status()}
    cold = service.query("cloud_summary", query_id="demo-cold")
    warm = service.query("cloud_summary", query_id="demo-warm")
    print("冷计算缓存命中:", cold["cache"]["hit"], " 热查询缓存命中:", warm["cache"]["hit"])
    try:
        service.query("error_stats", budget=0.0, query_id="demo-over-budget")
    except LatencyBudgetError as error:
        print("超预算 ->", error.code, "query_id:", error.payload["query_id"],
              "elapsed_ms:", error.payload["elapsed_ms"])

    print("\n演示完成。固定摘要：")
    print(dumps(fixed_summary).decode())
    return fixed_summary


if __name__ == "__main__":
    run_demo()
