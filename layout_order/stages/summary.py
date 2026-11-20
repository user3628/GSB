"""阶段四：结果汇总。

汇总块序列、跨栏统计、顺序置信度与页面级摘要；顺序质量指标
（相邻块逆序数、顺序准确率）仅在样本显式提供 reference_order 时计算，
参考信息缺失时明确置空，绝不从缺失信息推断。
"""

from __future__ import annotations

from typing import Any

from ..hashing import canonical_bytes, digest, digest_bytes


def run(
    document: dict[str, Any],
    normalized: dict[str, Any],
    column_result: dict[str, Any],
    order_result: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    doc_id = document["doc_id"]
    sequence = order_result["block_sequence"]
    produced_ids = [item["block_id"] for item in sequence]

    # 以原始框为口径的块覆盖检查（合并块展开为 source_box_ids）
    covered_source_ids: list[str] = []
    for item in sequence:
        covered_source_ids.extend(item["source_box_ids"])
    if sorted(covered_source_ids) != sorted(box["box_id"] for box in document["boxes"]):
        missing = sorted(
            set(box["box_id"] for box in document["boxes"]) - set(covered_source_ids)
        )
        from ..errors import LayoutError

        raise LayoutError(
            "E_ORDER_INVALID",
            f"文档 {doc_id} 块序列未完整覆盖原始框",
            {"doc_id": doc_id, "block_position": None, "missing_box_ids": missing,
             "reason": "incomplete_coverage"},
        )

    spanning_blocks = [item for item in sequence if item["spanning"]]
    spanning_stats = {
        "spanning_block_count": len(spanning_blocks),
        "spanning_block_ids": [item["block_id"] for item in spanning_blocks],
        "column_count": column_result["column_count"],
        "spanning_ratio": round(
            len(spanning_blocks) / max(1, len(sequence)), 6
        ),
    }

    order_quality = _order_quality(document, sequence)

    body_boxes = [box for box in normalized["boxes"] if box["role"] == "body"]
    page_summary = {
        "doc_id": doc_id,
        "page_width": document["page_width"],
        "page_height": document["page_height"],
        "tags": document.get("tags", []),
        "raw_box_count": len(document["boxes"]),
        "normalized_block_count": len(normalized["boxes"]),
        "merge_count": len(normalized["merge_records"]),
        "column_count": column_result["column_count"],
        "header_count": order_result["stats"]["header_count"],
        "footer_count": order_result["stats"]["footer_count"],
        "body_block_count": len(body_boxes),
        "spanning_block_count": len(spanning_blocks),
        "confidence_overall": order_result["confidence"]["overall"],
    }

    result = {
        "doc_id": doc_id,
        "page_summary": page_summary,
        "block_sequence": sequence,
        "spanning_stats": spanning_stats,
        "columns": column_result["columns"],
        "confidence": order_result["confidence"],
        "order_quality": order_quality,
    }
    # 最终结果的固定摘要：对规范化后的完整结果做 SHA-256
    result["result_digest"] = digest_bytes(canonical_bytes(result))
    return result


def _order_quality(
    document: dict[str, Any], sequence: list[dict[str, Any]]
) -> dict[str, Any]:
    reference = document.get("reference_order")
    if not reference:
        return {
            "reference_available": False,
            "inversion_count": None,
            "adjacent_inversion_count": None,
            "order_accuracy": None,
            "note": "样本未提供显式参考阅读顺序，指标不做推断",
        }

    block_to_min_rank: dict[str, int] = {}
    for rank, source_id in enumerate(reference):
        for item in sequence:
            if source_id in item["source_box_ids"]:
                block_id = item["block_id"]
                block_to_min_rank[block_id] = min(
                    rank, block_to_min_rank.get(block_id, rank)
                )
                break
    ranks = [block_to_min_rank[item["block_id"]] for item in sequence]

    inversion_count = 0
    for i in range(len(ranks)):
        for j in range(i + 1, len(ranks)):
            if ranks[i] > ranks[j]:
                inversion_count += 1
    adjacent_inversions = sum(
        1 for i in range(len(ranks) - 1) if ranks[i] > ranks[i + 1]
    )
    pair_total = len(ranks) * (len(ranks) - 1) // 2
    accuracy = 1.0 if pair_total == 0 else round(1.0 - inversion_count / pair_total, 6)
    return {
        "reference_available": True,
        "reference_order": list(reference),
        "rank_vector": ranks,
        "inversion_count": inversion_count,
        "adjacent_inversion_count": adjacent_inversions,
        "order_accuracy": accuracy,
        "pair_total": pair_total,
    }


def input_fingerprint(
    document: dict[str, Any],
    normalized: dict[str, Any],
    column_result: dict[str, Any],
    order_result: dict[str, Any],
) -> str:
    """阶段四输入指纹：前三阶段产物 + 参考顺序（参考仅影响质量指标）。"""
    content = {
        "doc_id": document["doc_id"],
        "reference_order": document.get("reference_order"),
        "tags": document.get("tags", []),
        "page_width": document["page_width"],
        "page_height": document["page_height"],
        "normalized": {
            "boxes": normalized["boxes"],
            "merge_records": normalized["merge_records"],
            "stats": normalized["stats"],
        },
        "column": {
            "column_count": column_result["column_count"],
            "columns": column_result["columns"],
            "assignments": column_result["assignments"],
        },
        "order": {
            "block_sequence": order_result["block_sequence"],
            "confidence": order_result["confidence"],
            "stats": order_result["stats"],
        },
    }
    return digest(content)
