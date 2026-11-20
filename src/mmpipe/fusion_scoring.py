"""阶段 3：融合打分。

融合分数由跨模态相似度、来源算子置信度与可配置权重合成：
    fusion = w_sim * ((similarity + 1) / 2)
           + w_op  * operator_confidence
（权重在配置中给出并参与配置版本指纹；权重变化必然改变分数。）

结论阈值可配置（strong_threshold / weak_threshold）：
    fusion >= strong            -> consistent
    weak <= fusion < strong     -> weak
    fusion < weak               -> conflict
"""

from .errors import E_MISSING_INPUT, E_NUMERIC, PipeError, StageFailure


def score_unit(visual_item: dict, config: dict) -> dict:
    """不可拆分样本单元：单条对齐结果 -> 融合打分记录。"""
    sample_id = visual_item["id"]
    weights = config["fusion_weights"]
    w_sim = float(weights["similarity"])
    w_op = float(weights["operator"])
    thresholds = config["fusion_thresholds"]
    strong = float(thresholds["strong"])
    weak = float(thresholds["weak"])
    confidence_map = config.get("operator_confidence")
    if not isinstance(confidence_map, dict) or not confidence_map:
        raise PipeError(E_MISSING_INPUT,
                        "operator_confidence mapping missing in config",
                        stage="fusion_scoring", sample_id=sample_id)
    operator = visual_item.get("operator")
    if operator not in confidence_map:
        raise PipeError(E_MISSING_INPUT,
                        f"no confidence configured for operator {operator!r}",
                        stage="fusion_scoring", sample_id=sample_id)
    op_conf = float(confidence_map[operator])
    if not (0.0 <= op_conf <= 1.0):
        raise PipeError(E_NUMERIC,
                        f"operator confidence out of [0,1]: {op_conf!r}",
                        stage="fusion_scoring", sample_id=sample_id)
    similarity = float(visual_item["similarity"])
    if similarity != similarity or similarity in (float("inf"), float("-inf")):
        raise PipeError(E_NUMERIC,
                        f"similarity non-finite: {similarity!r}",
                        stage="fusion_scoring", sample_id=sample_id)
    sim_component = (similarity + 1.0) / 2.0
    fusion = w_sim * sim_component + w_op * op_conf
    if fusion != fusion or fusion in (float("inf"), float("-inf")):
        raise PipeError(E_NUMERIC, f"fusion score non-finite: {fusion!r}",
                        stage="fusion_scoring", sample_id=sample_id)
    if fusion >= strong:
        label = "consistent"
    elif fusion >= weak:
        label = "weak"
    else:
        label = "conflict"
    return {
        "id": sample_id,
        "text_id": visual_item["text_id"],
        "operator": operator,
        "similarity": similarity,
        "operator_confidence": op_conf,
        "similarity_component": sim_component,
        "fusion_score": fusion,
        "label": label,
    }


def build_product(items: list[dict], config: dict) -> dict:
    counts = {"consistent": 0, "weak": 0, "conflict": 0}
    for item in items:
        counts[item["label"]] += 1
    return {
        "stage": "fusion_scoring",
        "items": items,
        "summary": {
            "record_count": len(items),
            "label_counts": counts,
            "weights": {
                "similarity": float(config["fusion_weights"]["similarity"]),
                "operator": float(config["fusion_weights"]["operator"]),
            },
            "thresholds": {
                "strong": float(config["fusion_thresholds"]["strong"]),
                "weak": float(config["fusion_thresholds"]["weak"]),
            },
        },
    }


def run(visual_product: dict, config: dict) -> dict:
    items: list[dict] = []
    errors: list[dict] = []
    for visual_item in visual_product["items"]:
        try:
            items.append(score_unit(visual_item, config))
        except PipeError as exc:
            errors.append(exc.to_dict())
    if errors:
        raise StageFailure("fusion_scoring", errors)
    return build_product(items, config)
