"""管线编排：四阶段执行、恢复复用、优雅停止与失效扫描。

设计要点：
- 各阶段期望输入指纹完全由原始输入与配置推导（无需依赖已发布产物），
  因此恢复/失效判断可在执行前完成；
- 每个阶段在线程池中按不可拆分样本单元执行，停止信号只在单元边界检查，
  收到停止后当前在途单元跑完、未开始单元取消，阶段不发布、下次整体重算；
- 单元错误按阶段聚合，任一单元失败则阶段失败且不发布检查点。
"""

import json
import threading
import time
import concurrent.futures

from . import STAGES, aggregation, fusion_scoring, text_encoding, visual_alignment
from .codec import fingerprint
from .errors import E_STOP_REQUESTED, PipeError, StageFailure

STAGE_CONFIG_KEYS = {
    "text_encoding": ("embedding_dim", "hash_algorithm", "text_normalization"),
    "visual_alignment": ("embedding_dim", "projection_matrix"),
    "fusion_scoring": ("fusion_weights", "fusion_thresholds", "operator_confidence"),
    "aggregation": ("fusion_weights", "fusion_thresholds"),
}


class StopController:
    """进程内事件 + stop.flag 文件双通道停止信号。"""

    def __init__(self, workspace):
        self.workspace = workspace
        self.event = threading.Event()

    def request(self) -> None:
        self.event.set()
        self.workspace.request_stop()

    def requested(self) -> bool:
        return self.event.is_set() or self.workspace.stop_requested()

    def clear(self) -> None:
        self.event.clear()
        self.workspace.clear_stop()


def load_texts(path: str) -> list[dict]:
    records: list[dict] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_images(path: str) -> list[dict]:
    with open(path, "rb") as handle:
        return json.loads(handle.read())


def stage_config(config: dict, stage: str) -> dict:
    return {key: config[key] for key in STAGE_CONFIG_KEYS[stage]}


def expected_inputs(config: dict, texts: list[dict], images: list[dict]) -> dict[str, dict]:
    """计算各阶段期望的输入指纹与阶段配置版本（纯函数，可复现）。"""
    ordered_texts = sorted(texts, key=lambda r: str(r.get("id")))
    ordered_images = sorted(images, key=lambda r: str(r.get("id")))
    text_payload = [
        {"id": r.get("id"), "text": r.get("text"), "lang": r.get("lang")}
        for r in ordered_texts
    ]
    image_payload = [
        {"id": r.get("id"), "feature": r.get("feature"),
         "text_id": r.get("text_id"), "operator": r.get("operator")}
        for r in ordered_images
    ]
    text_fp = fingerprint({"records": text_payload})
    visual_fp = fingerprint({
        "text_input": text_fp,
        "images": {"records": image_payload},
        "projection": fingerprint(config["projection_matrix"]),
    })
    fusion_fp = fingerprint({
        "visual_input": visual_fp,
        "operator_confidence": config["operator_confidence"],
    })
    aggregation_fp = fingerprint({
        "fusion_input": fusion_fp,
        "text_input": text_fp,
    })
    input_fp = {
        "text_encoding": text_fp,
        "visual_alignment": visual_fp,
        "fusion_scoring": fusion_fp,
        "aggregation": aggregation_fp,
    }
    return {
        stage: {"input": input_fp[stage],
                "config": fingerprint(stage_config(config, stage))}
        for stage in STAGES
    }


def scan_invalidations(workspace, expected: dict[str, dict]) -> list[dict]:
    """对照当前已发布检查点，识别最早失效阶段并级联失效。"""
    actions: list[dict] = []
    component_of = {
        "text_encoding": "text_input",
        "visual_alignment": "visual_input_or_projection",
        "fusion_scoring": "fusion_config_or_upstream",
        "aggregation": "aggregation_config_or_upstream",
    }
    for stage in STAGES:
        cur = workspace.stage_state(stage)
        if cur is None or cur.get("status") != "published":
            continue
        exp = expected[stage]
        if cur["input_fingerprint"] != exp["input"]:
            workspace.invalidate_from(stage, "input fingerprint changed",
                                      component_of[stage],
                                      cur["input_fingerprint"], exp["input"])
            actions.append({"stage": stage, "kind": "input",
                            "old": cur["input_fingerprint"],
                            "new": exp["input"]})
            break
        if cur["config_version"] != exp["config"]:
            workspace.invalidate_from(stage, "config version changed",
                                      component_of[stage],
                                      cur["config_version"], exp["config"])
            actions.append({"stage": stage, "kind": "config",
                            "old": cur["config_version"], "new": exp["config"]})
            break
    return actions


def _execute_stage(stage: str, units: list, worker, build_product,
                   config: dict, workspace, stop: StopController,
                   max_workers: int, unit_delay: float,
                   unit_gate=None) -> tuple[dict, list[dict]]:
    """线程池执行样本单元；返回 (产物, 错误列表)。停止在单元边界生效。

    unit_gate（可选）：每个单元开始前调用的零参数回调，用于演示场景注入
    停止信号；抛出 PipeError 时该单元记为错误并进入停止流程。
    """
    ordered = list(units)
    results: list = [None] * len(ordered)
    errors: list[dict] = []
    workspace.log_event("stage_start", stage=stage, units=len(ordered))

    def guarded(unit):
        if unit_gate is not None:
            unit_gate()
        if unit_delay:
            time.sleep(unit_delay)
        return worker(unit)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_index = {pool.submit(guarded, unit): idx
                        for idx, unit in enumerate(ordered)}
        pending = set(future_index)
        try:
            for future in concurrent.futures.as_completed(future_index):
                idx = future_index[future]
                try:
                    results[idx] = future.result()
                except PipeError as exc:
                    errors.append(exc.to_dict())
                pending.discard(future)
                if stop.requested():
                    for leftover in pending:
                        leftover.cancel()
                    raise PipeError(E_STOP_REQUESTED,
                                    "stop requested at unit boundary", stage=stage)
        finally:
            if stop.requested():
                for leftover in pending:
                    leftover.cancel()

    if errors:
        raise StageFailure(stage, errors)
    product = build_product(results, config)
    return product, errors


def _published_match(workspace, stage: str, expected_entry: dict) -> bool:
    cur = workspace.stage_state(stage)
    return (cur is not None and cur.get("status") == "published"
            and cur["input_fingerprint"] == expected_entry["input"]
            and cur["config_version"] == expected_entry["config"])


def run_pipeline(workspace, config: dict, texts: list[dict], images: list[dict],
                 stop: StopController | None = None,
                 max_workers: int | None = None,
                 unit_delay: float = 0.0,
                 unit_gate=None) -> dict:
    stop = stop or StopController(workspace)
    max_workers = max_workers or int(config.get("thread_pool_workers", 4))
    expected = expected_inputs(config, texts, images)
    invalidations = scan_invalidations(workspace, expected)
    report: dict = {"reused": [], "executed": [], "invalidations": invalidations,
                    "stopped": None, "published": [], "head_seq": None}

    # ---- 阶段 1：文本编码 ----
    if _published_match(workspace, "text_encoding", expected["text_encoding"]):
        report["reused"].append("text_encoding")
        text_product = workspace.load_current_product("text_encoding")
    else:
        text_product, _ = _execute_stage(
            "text_encoding", sorted(texts, key=lambda r: str(r.get("id"))),
            lambda unit: text_encoding.encode_unit(unit, config),
            text_encoding.build_product, config, workspace, stop,
            max_workers, unit_delay, unit_gate)
        cp = workspace.publish("text_encoding", text_product,
                               expected["text_encoding"]["input"],
                               expected["text_encoding"]["config"])
        report["executed"].append("text_encoding")
        report["published"].append(cp)

    # ---- 阶段 2：视觉对齐 ----
    if _published_match(workspace, "visual_alignment", expected["visual_alignment"]):
        report["reused"].append("visual_alignment")
        visual_product = workspace.load_current_product("visual_alignment")
    else:
        visual_product, _ = _execute_stage(
            "visual_alignment", sorted(images, key=lambda r: str(r.get("id"))),
            lambda unit: visual_alignment.align_unit(unit, text_product, config),
            visual_alignment.build_product, config, workspace, stop,
            max_workers, unit_delay, unit_gate)
        cp = workspace.publish("visual_alignment", visual_product,
                               expected["visual_alignment"]["input"],
                               expected["visual_alignment"]["config"])
        report["executed"].append("visual_alignment")
        report["published"].append(cp)

    # ---- 阶段 3：融合打分 ----
    if _published_match(workspace, "fusion_scoring", expected["fusion_scoring"]):
        report["reused"].append("fusion_scoring")
        fusion_product = workspace.load_current_product("fusion_scoring")
    else:
        fusion_product, _ = _execute_stage(
            "fusion_scoring", visual_product["items"],
            lambda unit: fusion_scoring.score_unit(unit, config),
            fusion_scoring.build_product, config, workspace, stop,
            max_workers, unit_delay, unit_gate)
        cp = workspace.publish("fusion_scoring", fusion_product,
                               expected["fusion_scoring"]["input"],
                               expected["fusion_scoring"]["config"])
        report["executed"].append("fusion_scoring")
        report["published"].append(cp)

    # ---- 阶段 4：聚合 ----
    if _published_match(workspace, "aggregation", expected["aggregation"]):
        report["reused"].append("aggregation")
        aggregate_product = workspace.load_current_product("aggregation")
    else:
        fusion_by_text: dict[str, list[dict]] = {}
        for item in fusion_product["items"]:
            fusion_by_text.setdefault(item["text_id"], []).append(item)
        aggregate_product, _ = _execute_stage(
            "aggregation", text_product["items"],
            lambda unit: aggregation.aggregate_unit(unit, fusion_by_text),
            lambda samples, _cfg: aggregation.build_product(
                samples, expected, STAGES),
            config, workspace, stop, max_workers, unit_delay, unit_gate)
        cp = workspace.publish("aggregation", aggregate_product,
                               expected["aggregation"]["input"],
                               expected["aggregation"]["config"])
        report["executed"].append("aggregation")
        report["published"].append(cp)

    report["head_seq"] = workspace.head_seq()
    report["stop_requested"] = stop.requested()
    workspace.log_event("pipeline_complete", head_seq=report["head_seq"],
                        reused=report["reused"], executed=report["executed"])
    return report
