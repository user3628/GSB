"""阶段 4：结果聚合。

按文本样本汇总其全部图像描述的协同结论：
- conclusion: 所有图像一致 -> consistent；存在冲突 -> conflict；否则 weak；
- total_confidence: 关联图像融合分数的均值（[0,1]）；
- 参与阶段摘要（含各阶段输入指纹与配置版本）；
冲突样本显式保留并标注，绝不静默丢弃。
"""


def _conclude(labels: list[str]) -> str:
    if "conflict" in labels:
        return "conflict"
    if labels and all(label == "consistent" for label in labels):
        return "consistent"
    return "weak"


def aggregate_unit(text_item: dict, fusion_by_text: dict) -> dict:
    """不可拆分样本单元：单个文本样本的协同结论汇总。"""
    text_id = text_item["id"]
    linked = sorted(fusion_by_text.get(text_id, []), key=lambda x: x["id"])
    labels = [item["label"] for item in linked]
    scores = [item["fusion_score"] for item in linked]
    return {
        "text_id": text_id,
        "lang": text_item.get("lang"),
        "image_count": len(linked),
        "image_ids": [item["id"] for item in linked],
        "conclusion": _conclude(labels),
        "total_confidence": (sum(scores) / len(scores)) if scores else 0.0,
        "labels": labels,
        "has_conflict": "conflict" in labels,
    }


def build_product(samples: list[dict], expected: dict, stages: tuple) -> dict:
    conclusion_counts = {"consistent": 0, "weak": 0, "conflict": 0}
    for sample in samples:
        conclusion_counts[sample["conclusion"]] += 1
    return {
        "stage": "aggregation",
        "samples": samples,
        "summary": {
            "sample_count": len(samples),
            "conclusion_counts": conclusion_counts,
            "conflict_sample_ids": [s["text_id"] for s in samples if s["has_conflict"]],
            "stage_participation": [
                {
                    "stage": name,
                    "input_fingerprint": expected[name]["input"],
                    "config_version": expected[name]["config"],
                }
                for name in stages
            ],
        },
    }


def run(fusion_product: dict, text_product: dict, expected: dict,
        stages: tuple) -> dict:
    fusion_by_text: dict[str, list[dict]] = {}
    for item in fusion_product["items"]:
        fusion_by_text.setdefault(item["text_id"], []).append(item)
    samples = [aggregate_unit(text_item, fusion_by_text)
               for text_item in text_product["items"]]
    return build_product(samples, expected, stages)
