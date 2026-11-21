"""管线编排：检查点复用、增量失效传播、优雅停止与一致性校验。"""

import os
import shutil
import tempfile

from pipeline import STAGES
from pipeline.checkpoints import CheckpointStore
from pipeline.config import config_fingerprint, load_config
from pipeline.errors import E_INVALID_STAGE, E_INCREMENTAL_MISMATCH, PipelineError
from pipeline.stages import (
    run_stage1,
    run_stage2,
    run_stage3,
    run_stage4,
    stage2_input_fingerprint,
    stage3_input_fingerprint,
    stage4_input_fingerprint,
    stage_config_fingerprint,
    train_input_fingerprint,
    write_artifact,
    load_artifact,
)
from pipeline.workspace import Workspace

STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}


class StopRequested(Exception):
    """优雅停止信号：当前不可拆分单元完成后抛出。"""


class Pipeline:
    def __init__(self, workspace_root):
        self.ws = Workspace(workspace_root).ensure_dirs()
        self.store = CheckpointStore(self.ws)

    # ------------------------------------------------------------------ 基础
    def _config(self):
        if not os.path.exists(self.ws.config_path):
            raise PipelineError(E_INVALID_STAGE, "缺少管线配置文件", {"path": self.ws.config_path})
        return load_config(self.ws.config_path)

    def import_file(self, source_path, kind):
        """导入训练分片(jsonl)/推理分片(json)/配置(yaml)。"""
        with self.ws.lock():
            if kind == "train":
                destination = os.path.join(self.ws.train_dir, os.path.basename(source_path))
            elif kind == "infer":
                destination = os.path.join(self.ws.infer_dir, os.path.basename(source_path))
            elif kind == "config":
                destination = self.ws.config_path
            else:
                raise ValueError(f"未知导入类型: {kind}")
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            shutil.copyfile(source_path, destination)
            return destination

    def _records_by_stage(self):
        records, _ = self.store.load_valid_records()
        return {record["stage"]: record for record in records}, records

    def _artifact_exists(self, stage):
        return os.path.exists(self.ws.artifact_path(stage))

    # ------------------------------------------------------------ 失效判断
    def _earliest_invalid_stage(self, config):
        """返回 (最早失效阶段下标, 原因列表)；全部有效返回 (None, [])。"""
        by_stage, records = self._records_by_stage()
        if not records:
            return 0, [{"stage": STAGES[0], "reason": "NO_CHECKPOINT"}]
        reasons = []

        record1 = by_stage.get(STAGES[0])
        train_fp, _ = train_input_fingerprint(self.ws)
        cfg1 = stage_config_fingerprint(config, STAGES[0])
        if record1 is None or not self._artifact_exists(STAGES[0]):
            return 0, [{"stage": STAGES[0], "reason": "MISSING_ARTIFACT"}]
        if record1["input_fingerprint"] != train_fp or record1["config_fingerprint"] != cfg1:
            reason = "TRAIN_DATA_CHANGED" if record1["input_fingerprint"] != train_fp else "CONFIG_CHANGED"
            return 0, [{"stage": STAGES[0], "reason": reason}]

        stage1_artifact = load_artifact(self.ws.artifact_path(STAGES[0]))
        fp2 = stage2_input_fingerprint(stage1_artifact, config)
        cfg2 = stage_config_fingerprint(config, STAGES[1])
        record2 = by_stage.get(STAGES[1])
        if record2 is None or not self._artifact_exists(STAGES[1]):
            return 1, [{"stage": STAGES[1], "reason": "MISSING_ARTIFACT"}]
        if record2["input_fingerprint"] != fp2 or record2["config_fingerprint"] != cfg2:
            reason = (
                "FEATURE_ARTIFACT_CHANGED"
                if record2["input_fingerprint"] != fp2
                else "MODEL_CONFIG_CHANGED"
            )
            return 1, [{"stage": STAGES[1], "reason": reason}]

        stage2_artifact = load_artifact(self.ws.artifact_path(STAGES[1]))
        fp3 = stage3_input_fingerprint(self.ws, stage2_artifact, config)
        cfg3 = stage_config_fingerprint(config, STAGES[2])
        record3 = by_stage.get(STAGES[2])
        if record3 is None or not self._artifact_exists(STAGES[2]):
            return 2, [{"stage": STAGES[2], "reason": "MISSING_ARTIFACT"}]
        if record3["input_fingerprint"] != fp3 or record3["config_fingerprint"] != cfg3:
            if record3["config_fingerprint"] != cfg3 and record3["input_fingerprint"] == fp3:
                reason = "INFERENCE_CONFIG_CHANGED"
            else:
                reason = "MODEL_OR_INFER_INPUT_CHANGED"
            return 2, [{"stage": STAGES[2], "reason": reason}]

        stage3_artifact = load_artifact(self.ws.artifact_path(STAGES[2]))
        fp4 = stage4_input_fingerprint(stage3_artifact, config)
        cfg4 = stage_config_fingerprint(config, STAGES[3])
        record4 = by_stage.get(STAGES[3])
        if record4 is None or not self._artifact_exists(STAGES[3]):
            return 3, [{"stage": STAGES[3], "reason": "MISSING_ARTIFACT"}]
        if record4["input_fingerprint"] != fp4 or record4["config_fingerprint"] != cfg4:
            reason = (
                "INFERENCE_RESULT_CHANGED"
                if record4["input_fingerprint"] != fp4
                else "CONFIG_CHANGED"
            )
            return 3, [{"stage": STAGES[3], "reason": reason}]
        return None, reasons

    def _invalidate(self, start_index, reason):
        removed = self.store.truncate_from_stage(start_index, reason)
        for event in removed:
            self.store.append_invalidation_event(
                {
                    "event_id": len(self.store.read_invalidation_events()) + 1,
                    "stage": event["stage"],
                    "sequence": event["sequence"],
                    "reason": reason,
                    "action": "QUARANTINED_AND_REBUILD",
                }
            )
            artifact_path = self.ws.artifact_path(event["stage"])
            if os.path.exists(artifact_path):
                os.remove(artifact_path)

    # ------------------------------------------------------------ 主执行
    def run(self, through_stage=STAGES[-1], stop_event=None):
        """从最近一致检查点继续，执行到 through_stage。返回执行报告。"""
        if through_stage not in STAGE_INDEX:
            raise PipelineError(E_INVALID_STAGE, f"未知阶段: {through_stage}")
        target = STAGE_INDEX[through_stage]
        report = {"reused": [], "rebuilt": [], "invalidated": [], "stopped": False}
        with self.ws.lock():
            config = self._config()
            earliest, reasons = self._earliest_invalid_stage(config)
            if earliest is not None and earliest <= target:
                self._invalidate(earliest, reasons[0]["reason"])
                report["invalidated"] = reasons

            for index in range(target + 1):
                stage = STAGES[index]
                by_stage, _ = self._records_by_stage()
                if stage in by_stage and self._artifact_exists(stage):
                    report["reused"].append(stage)
                    continue
                if stop_event is not None and stop_event.is_set():
                    report["stopped"] = True
                    return report
                if index > 0:
                    prev = STAGES[index - 1]
                    prev_by_stage, _ = self._records_by_stage()
                    if prev not in prev_by_stage or not self._artifact_exists(prev):
                        raise PipelineError(
                            E_INVALID_STAGE,
                            f"缺失前置阶段 {prev}，不能执行 {stage}",
                            {"requested_stage": stage, "missing_stage": prev},
                        )
                artifact, input_fp, summary, _ = self._execute_stage(stage, config, stop_event)
                write_artifact(self.ws, stage, artifact)
                self.store.publish(
                    stage, input_fp, stage_config_fingerprint(config, stage), summary
                )
                report["rebuilt"].append(stage)
                if stop_event is not None and stop_event.is_set():
                    report["stopped"] = True
                    return report
            return report

    def _execute_stage(self, stage, config, stop_event):
        if stage == STAGES[0]:
            return run_stage1(self.ws, config)
        if stage == STAGES[1]:
            stage1 = load_artifact(self.ws.artifact_path(STAGES[0]))
            return run_stage2(stage1, config)
        if stage == STAGES[2]:
            stage2 = load_artifact(self.ws.artifact_path(STAGES[1]))
            return run_stage3(self.ws, stage2, config)
        stage3 = load_artifact(self.ws.artifact_path(STAGES[2]))
        return run_stage4(stage3, config)

    # ------------------------------------------------------------ 查询
    def status(self):
        with self.ws.lock():
            records, quarantined = self.store.load_valid_records()
            cfg_fp = None
            if os.path.exists(self.ws.config_path):
                cfg_fp = config_fingerprint(load_config(self.ws.config_path))
            train_fp, train_files = (None, [])
            if os.path.isdir(self.ws.train_dir):
                train_fp, train_files = train_input_fingerprint(self.ws)
            return {
                "workspace": self.ws.root,
                "stages": list(STAGES),
                "checkpoints": records,
                "next_sequence": len(records) + 1,
                "quarantined": quarantined,
                "invalidation_events": self.store.read_invalidation_events(),
                "config_fingerprint": cfg_fp,
                "train_fingerprint": train_fp,
                "train_files": train_files,
                "artifacts": {
                    stage: {
                        "path": self.ws.artifact_path(stage),
                        "exists": os.path.exists(self.ws.artifact_path(stage)),
                    }
                    for stage in STAGES
                },
            }

    # ----------------------------------------------------- 增量 vs 全量校验
    def validate_incremental(self, scope=STAGES[-1]):
        """在临时工作区从零全量构建，与当前增量工作区逐字节比对。"""
        if scope not in STAGE_INDEX:
            raise PipelineError(E_INVALID_STAGE, f"未知校验范围: {scope}")
        target = STAGE_INDEX[scope]
        with self.ws.lock():
            current_records, _ = self.store.load_valid_records()
            needed = STAGES[: target + 1]
            for index, stage in enumerate(needed):
                if index + 1 > len(current_records) or not self._artifact_exists(stage):
                    raise PipelineError(
                        E_INVALID_STAGE,
                        f"当前工作区缺少阶段 {stage} 产物，无法校验",
                        {"missing_stage": stage},
                    )
            tmp_root = tempfile.mkdtemp(prefix="fullbuild-", dir=self.ws.tmp_dir)
            try:
                fresh = Workspace(tmp_root).ensure_dirs()
                shutil.copytree(self.ws.train_dir, fresh.train_dir, dirs_exist_ok=True)
                shutil.copytree(self.ws.infer_dir, fresh.infer_dir, dirs_exist_ok=True)
                os.makedirs(os.path.dirname(fresh.config_path), exist_ok=True)
                shutil.copyfile(self.ws.config_path, fresh.config_path)
                Pipeline(tmp_root).run(through_stage=scope)
                comparisons = []
                for index, stage in enumerate(needed):
                    name = stage + ".json"
                    current_path = self.ws.artifact_path(stage)
                    fresh_path = fresh.artifact_path(stage)
                    with open(current_path, "rb") as handle:
                        current_bytes = handle.read()
                    with open(fresh_path, "rb") as handle:
                        fresh_bytes = handle.read()
                    match = current_bytes == fresh_bytes
                    comparisons.append(
                        {
                            "stage": stage,
                            "artifact_name": name,
                            "byte_identical": match,
                        }
                    )
                    if not match:
                        raise PipelineError(
                            E_INCREMENTAL_MISMATCH,
                            "增量重建产物与全量构建产物逐字节不一致",
                            {
                                "first_mismatch_stage": stage,
                                "first_mismatch_artifact": name,
                                "comparisons": comparisons,
                            },
                        )
                fresh_records, _ = CheckpointStore(fresh).load_valid_records()
                current_prefix = current_records[: len(needed)]
                sequences = [record["sequence"] for record in current_prefix]
                if sequences != list(range(1, len(needed) + 1)):
                    raise PipelineError(
                        E_INCREMENTAL_MISMATCH,
                        "检查点序号不连续",
                        {"sequences": sequences},
                    )
                for current_record, fresh_record in zip(current_prefix, fresh_records):
                    if current_record != fresh_record:
                        raise PipelineError(
                            E_INCREMENTAL_MISMATCH,
                            "增量与全量检查点记录不一致",
                            {
                                "first_mismatch_stage": current_record["stage"],
                                "first_mismatch_artifact": "checkpoint:" + current_record["stage"],
                            },
                        )
                return {"scope": scope, "byte_identical": True, "comparisons": comparisons}
            finally:
                shutil.rmtree(tmp_root, ignore_errors=True)

    # ------------------------------------------------------------ 并发发布
    def publish_raw(self, stage, input_fingerprint, config_fingerprint, artifact_summary):
        """供并发保护演示使用的底层发布入口（带锁 + 序号比较）。"""
        with self.ws.lock():
            return self.store.publish(stage, input_fingerprint, config_fingerprint, artifact_summary)
