"""End-to-end deterministic demonstration of the OCR diagnostic workbench.

Running this script (``python3 run_demo.py``) exercises:
  1. deterministic three-tier sample generation (small + medium + large),
  2. a clean four-stage run producing a fixed summary digest,
  3. a graceful stop followed by a resume that reproduces the clean digest,
  4. an invalid stage request returning the stable E_INVALID_STAGE code,
  5. a config change driving incremental invalidation and a verified rebuild.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile

from ocrwb.checkpoint import CheckpointStore
from ocrwb.config import WorkbenchConfig
from ocrwb.errors import InvalidStage
from ocrwb.hashing import canonical_json
from ocrwb.pipeline import Pipeline
from ocrwb.samplegen.generator import generate_dataset


def heading(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def main() -> None:
    root = tempfile.mkdtemp(prefix="ocrwb_demo_")
    sample_dir = os.path.join(root, "samples")
    workspace = os.path.join(root, "workspace")
    print("workspace root:", root)

    heading("1. 确定性样本生成（小/中/大三档，中英混排 + 倾斜）")
    manifest = generate_dataset(sample_dir)
    for group in manifest["groups"]:
        print(
            f"  {group['name']:<6} shards={group['shard_count']:<3} "
            f"total_chars={group['total_chars']}"
        )
    languages = sorted({language for shard in manifest["shards"] for language in shard["languages"]})
    tilted = sum(1 for shard in manifest["shards"] for truth in shard["truth"] if truth["tilt"])
    print("  languages:", languages, "| tilted truth lines:", tilted)

    pipeline = Pipeline(workspace, sample_dir, WorkbenchConfig())
    pipeline.import_sample(manifest)

    heading("2. 从零完整运行四阶段（检测 -> 取舍 -> 识别 -> 诊断）")
    clean_stats = pipeline.run()
    clean_report = pipeline.load_report()
    print("  rebuilt artifacts:", clean_stats.rebuilt_artifacts)
    print("  fixed summary digest:", clean_report["summary_digest"])
    print("  global metrics:", canonical_json(clean_report["global"]))
    print("  strategy hits:", canonical_json(clean_report["strategy_hits"]))

    heading("3. 优雅停止后恢复，结果与从零全量逐字节一致")
    stop_workspace = os.path.join(root, "workspace_stop")
    stop_pipeline = Pipeline(stop_workspace, sample_dir, WorkbenchConfig())
    stop_pipeline.import_sample(manifest)
    stop_pipeline.request_stop()
    stopped = stop_pipeline.run()
    partial = stop_pipeline.store.status()
    completed = sum(1 for shard in partial["shards"] if shard["published_stages"])
    print("  stopped flag honored:", stopped.stopped, "| completed shards:", completed)
    stop_pipeline.clear_stop()
    stop_pipeline.run()
    resumed_report = stop_pipeline.load_report()
    print(
        "  resumed digest equals clean run:",
        resumed_report["summary_digest"] == clean_report["summary_digest"],
    )

    heading("4. 非法阶段请求返回稳定错误码 E_INVALID_STAGE")
    store = CheckpointStore(workspace)
    image_id = "fresh_shard_without_prerequisites"
    try:
        with store.shard_lock(image_id):
            store._publish_locked(
                image_id,
                "recognize",
                1,
                "input",
                "config",
                None,
                "fp",
                {"illegal": True},
            )
    except InvalidStage as exc:
        print(" ", canonical_json(exc.to_dict()))

    heading("5. 配置变化触发增量失效并验证与全量重建一致")
    changed_config = WorkbenchConfig()
    object.__setattr__(changed_config, "binarize", type(changed_config.binarize)(threshold=120))
    incremental = Pipeline(workspace, sample_dir, changed_config)
    incremental.store.atomic_write_json(incremental.store.config_path, changed_config.to_dict())
    stats = incremental.run()
    affected = sorted({item["image_id"] for item in stats.invalidations})
    print("  invalidated shards:", len(affected), "| earliest stage:", stats.invalidations[0]["from_stage"])
    clean_changed = os.path.join(root, "workspace_clean_changed")
    reference = Pipeline(clean_changed, sample_dir, changed_config)
    reference.import_sample(manifest)
    reference.run()
    incremental.verify_incremental_equals_full(reference.load_report())
    print("  E_INCREMENTAL_MISMATCH absent: incremental == full rebuild")

    heading("6. 导出候选框与识别结果")
    export_path = os.path.join(root, "export.json")
    store = incremental.store
    export_payload = []
    for shard in manifest["shards"]:
        decision = store.load_artifact(shard["image_id"], "decision")
        recognition = store.load_artifact(shard["image_id"], "recognize")
        export_payload.append(
            {
                "image_id": shard["image_id"],
                "candidates": decision["refined_detection"]["candidates"],
                "recognized": recognition["recognition"]["results"],
            }
        )
    with open(export_path, "w", encoding="utf-8") as handle:
        handle.write(canonical_json(export_payload))
    print("  exported shards:", len(export_payload), "->", export_path)

    shutil.rmtree(root, ignore_errors=True)
    print("\nDemo finished successfully.")


if __name__ == "__main__":
    main()
