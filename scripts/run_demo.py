"""端到端确定性演示。

覆盖：
  1. 生成 3 个训练分片 + 2 个推理分片（中英混排、空样本）与 YAML 配置；
  2. 从零全量跑通四阶段，输出检查点、指纹、模型版本与汇总；
  3. 单样本编辑 -> 仅失效链路重建 -> 与全量逐字节比对；
  4. 单分片替换 -> 再次增量校验；
  5. 检查点损坏/截断恢复，序号连续无重复；
  6. 错误码行为：空样本、非法训练参数、模型版本不一致、缺失前置阶段、
     过期/错位检查点发布；
  7. 多进程并发发布保护。
"""

import json
import multiprocessing
import os
import shutil
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline import STAGES
from pipeline.atomicio import read_json
from pipeline.config import load_config
from pipeline.data_ops import (
    edit_training_sample,
    refresh_inference_model_versions,
    tamper_model_version_in_request,
)
from pipeline.errors import PipelineError
from pipeline.generator import generate_workspace
from pipeline.pipeline import Pipeline
from pipeline.shards import read_jsonl, write_jsonl


def emit(title, payload):
    print("\n=== " + title + " ===")
    if isinstance(payload, (dict, list)):
        print(json.dumps(payload, sort_keys=True, ensure_ascii=False, indent=2))
    else:
        print(payload)


def expect_error(title, func, expected_code):
    try:
        func()
    except PipelineError as exc:
        status = "OK" if exc.code == expected_code else "UNEXPECTED"
        emit(f"{title} [{status}: {exc.code}]", exc.to_dict())
        return exc
    raise AssertionError(f"{title} 未抛出预期错误 {expected_code}")


def _concurrent_publisher(workspace_root, stage, seq_stage_name, queue):
    try:
        pipe = Pipeline(workspace_root)
        record = pipe.publish_raw(
            stage,
            "fp-demo",
            "cfg-demo",
            {"artifact_name": stage + ".json", "digest": "deadbeef"},
        )
        queue.put(("accepted", record["sequence"], seq_stage_name))
    except PipelineError as exc:
        queue.put(("rejected", exc.code, seq_stage_name))


def main():
    base_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "demo-workspace")
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    workspace = os.path.join(base_dir, "main")

    # 1. 生成确定性数据
    generated = generate_workspace(workspace)
    emit(
        "1. 确定性样本生成",
        {
            "train_shards": [os.path.basename(p) for p in generated["train_paths"]],
            "infer_shards": [os.path.basename(p) for p in generated["infer_paths"]],
            "expected_model_version": generated["expected_model_version"],
        },
    )

    # 2. 全量运行
    pipe = Pipeline(workspace)
    report = pipe.run()
    emit("2. 首次全量运行（全部重建）", report)
    status = pipe.status()
    emit(
        "检查点与指纹",
        {
            "sequences": [c["sequence"] for c in status["checkpoints"]],
            "stages": [c["stage"] for c in status["checkpoints"]],
            "artifact_summaries": [c["artifact_summary"] for c in status["checkpoints"]],
            "train_fingerprint": status["train_fingerprint"],
            "config_fingerprint": status["config_fingerprint"],
        },
    )
    summary = read_json(pipe.ws.artifact_path(STAGES[3]))[0]
    emit("阶段4 汇总结果", summary)

    # 3. 幂等：再次运行不应重建任何阶段
    rerun = pipe.run()
    emit("3. 重复运行（期望全部复用）", rerun)
    assert rerun["rebuilt"] == []

    # 4. 单样本编辑 -> 增量失效重建 -> 全量一致性校验
    edit_training_sample(pipe.ws, "train-0003", new_text="这个商品真的非常好用我很喜欢", new_label=1)
    pipe.run(through_stage=STAGES[1])
    refresh_inference_model_versions(pipe.ws)
    edit_report = pipe.run()
    emit("4. 单样本编辑后的失效重建", edit_report)
    validation = pipe.validate_incremental()
    emit("增量 vs 全量逐字节校验（全链路）", validation)
    assert validation["byte_identical"] is True
    stage_scope = pipe.validate_incremental(scope=STAGES[1])
    emit("按阶段范围校验（截至 stage2_model）", stage_scope)

    # 5. 单分片替换
    shard_path = os.path.join(pipe.ws.train_dir, "train-shard-02.jsonl")
    records = read_jsonl(shard_path)
    records[0]["text"] = "delivery service is terrible and broken"
    records[0]["label"] = 0
    write_jsonl(shard_path, records)
    pipe.run(through_stage=STAGES[1])
    refresh_inference_model_versions(pipe.ws)
    replace_report = pipe.run()
    emit("5. 单分片替换后的失效重建", replace_report)
    emit("再次全链路一致性校验", pipe.validate_incremental())

    # 6. 损坏检查点恢复：截断最后一条记录并制造半写文件
    last_record = os.path.join(pipe.ws.checkpoint_dir, "record-000004.json")
    os.remove(last_record)
    half_written = os.path.join(pipe.ws.checkpoint_dir, "record-000004.json")
    with open(half_written, "wb") as handle:
        handle.write(b'{"sequence": 4, "stage": "stage4_sum')  # 半写 JSON
    recovered = pipe.run()
    emit("6. 损坏/半写检查点恢复", recovered)
    status = pipe.status()
    sequences = [c["sequence"] for c in status["checkpoints"]]
    assert sequences == [1, 2, 3, 4], sequences
    emit("恢复后检查点序号（期望 1..4 连续）", {"sequences": sequences, "quarantined": status["quarantined"]})
    emit("恢复后全量一致性校验", pipe.validate_incremental())

    # 7a. E_EMPTY_SAMPLE 诊断保留
    stage1 = read_json(pipe.ws.artifact_path(STAGES[0]))[0]
    emit(
        "7a. 空样本诊断 E_EMPTY_SAMPLE",
        {"diagnostics": stage1["diagnostics"], "term_stats_head": stage1["term_stats"][:5]},
    )
    assert all(d["error"] == "E_EMPTY_SAMPLE" for d in stage1["diagnostics"])

    # 7b. 非法训练参数 E_INVALID_TRAIN / E_NUMERIC
    bad_root = os.path.join(base_dir, "bad-epochs")
    generate_workspace(bad_root)
    bad_pipe = Pipeline(bad_root)
    cfg = load_config(bad_pipe.ws.config_path)
    cfg["model"]["epochs"] = 0
    import yaml

    with open(bad_pipe.ws.config_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(cfg, handle, allow_unicode=True)
    expect_error("7b. epochs=0 -> E_INVALID_TRAIN", lambda: bad_pipe.run(), "E_INVALID_TRAIN")

    cfg["model"]["epochs"] = 8
    cfg["model"]["learning_rate"] = -0.5
    with open(bad_pipe.ws.config_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(cfg, handle, allow_unicode=True)
    expect_error("7c. 负学习率 -> E_INVALID_TRAIN", lambda: bad_pipe.run(), "E_INVALID_TRAIN")

    # 7d. 模型版本不一致 E_MODEL_VERSION_MISMATCH
    mismatch_root = os.path.join(base_dir, "mismatch")
    generate_workspace(mismatch_root)
    mismatch_pipe = Pipeline(mismatch_root)
    mismatch_pipe.run(through_stage=STAGES[1])
    tamper_model_version_in_request(mismatch_pipe.ws, "req-0001", "mv_deadbeefdeadbeef")
    expect_error(
        "7d. 引用错误模型版本 -> E_MODEL_VERSION_MISMATCH",
        lambda: mismatch_pipe.run(through_stage=STAGES[2]),
        "E_MODEL_VERSION_MISMATCH",
    )

    # 7e. 缺失前置阶段 E_INVALID_STAGE
    fresh_root = os.path.join(base_dir, "missing-stage")
    os.makedirs(fresh_root, exist_ok=True)
    fresh_pipe = Pipeline(fresh_root)
    expect_error(
        "7e. 缺失前置阶段 -> E_INVALID_STAGE",
        lambda: fresh_pipe.run(through_stage=STAGES[2]),
        "E_INVALID_STAGE",
    )

    # 7f. 并发发布保护 E_STALE_CHECKPOINT
    lock_root = os.path.join(base_dir, "lock-demo")
    generate_workspace(lock_root)
    lock_pipe = Pipeline(lock_root)
    lock_pipe.run(through_stage=STAGES[0])
    queue = multiprocessing.Queue()
    workers = [
        multiprocessing.Process(
            target=_concurrent_publisher,
            args=(lock_root, STAGES[1], "p1", queue),
        ),
        multiprocessing.Process(
            target=_concurrent_publisher,
            args=(lock_root, STAGES[1], "p2", queue),
        ),
        multiprocessing.Process(
            target=_concurrent_publisher,
            args=(lock_root, STAGES[3], "cross-stage", queue),
        ),
    ]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    outcomes = [queue.get() for _ in workers]
    emit("7f. 多进程并发发布保护（仅 1 个成功，其余 E_STALE_CHECKPOINT）", outcomes)
    accepted = [item for item in outcomes if item[0] == "accepted"]
    rejected = [item for item in outcomes if item[0] == "rejected"]
    assert len(accepted) == 1 and all(code == "E_STALE_CHECKPOINT" for _, code, _ in rejected)

    emit("演示完成", {"workspace": workspace, "all_checks": "passed"})


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
