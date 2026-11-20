"""固定随机种子的本地确定性版面样本生成器。

不访问任何外部资源；随机数仅用于文本片段挑选与毫米级安全抖动，
版面结构（栏、间隙、页眉页脚位置）保持确定，保证跨运行逐字节一致。
"""

from __future__ import annotations

import random
from typing import Any

SEED = 20261121
PAGE_W = 794.0  # A4 @96dpi 宽（像素）
PAGE_H = 1123.0

_ZH_SNIPPETS = [
    "阅读顺序分析是文档理解的基础环节",
    "版面元素经规范化后进入栏结构推断",
    "相邻文本框依据间隙阈值执行合并",
    "通栏标题需要单独标记并参与裁决",
    "检查点以原子方式写入工作区目录",
    "版本指纹变化会触发下游增量失效",
    "页眉页脚依据页边距离与字号识别",
    "顺序置信度由垂直间隙与栏归属计算",
]
_EN_SNIPPETS = [
    "Reading order detection starts with layout normalization",
    "Adjacent boxes are merged when the gap is below threshold",
    "Column intervals must not overlap and cover the body width",
    "Spanning blocks are arbitrated with configurable priority",
    "Atomic checkpoints survive interruption without partial writes",
    "Content fingerprints drive incremental invalidation downstream",
    "Header and footer boxes are classified by margin and font tier",
    "Order confidence combines vertical gap and column membership",
]
_MIXED_SNIPPETS = [
    "中英混排 Mixed-Line 的文本框需要稳定排序",
    "片段 fragment 合并后仍保持 deterministic 输出",
    "置信度 confidence 与逆序数 inversion 同时报告",
]


def _box(
    box_id: str,
    x0: float,
    y0: float,
    x1: float,
    y1: float,
    font_tier: str,
    text: str,
    lang: str,
) -> dict[str, Any]:
    for value in (x0, y0, x1, y1):
        if not 0.0 <= value <= 1.0:
            raise ValueError(f"归一化坐标越界: {box_id} -> {value}")
    if x1 <= x0 or y1 <= y0:
        raise ValueError(f"零面积框: {box_id}")
    return {
        "box_id": box_id,
        "bbox_norm": [round(x0, 4), round(y0, 4), round(x1, 4), round(y1, 4)],
        "font_tier": font_tier,
        "text": text,
        "lang": lang,
    }


def _jitter(rng: random.Random, span: float = 0.0015) -> float:
    return round(rng.uniform(-span, span), 4)


def _doc_single_zh(rng: random.Random) -> dict[str, Any]:
    boxes = [
        _box("d1-b01", 0.10, 0.020, 0.90, 0.044, "small", "学术期刊 第 42 卷 第 3 期", "zh"),
        _box("d1-b02", 0.10, 0.080, 0.90, 0.118, "xlarge", "单栏中文文档版面分析", "zh"),
    ]
    y = 0.160
    for idx in range(4):
        text = _ZH_SNIPPETS[idx] + "。" + _ZH_SNIPPETS[idx + 4]
        boxes.append(
            _box(
                f"d1-b{idx + 3:02d}",
                0.10 + _jitter(rng),
                y,
                0.90 + _jitter(rng),
                round(y + 0.135, 4),
                "body",
                text,
                "zh",
            )
        )
        y = round(y + 0.175, 4)
    boxes.append(_box("d1-b07", 0.10, 0.945, 0.90, 0.966, "xsmall", "第 1 页 共 1 页", "zh"))
    reference = ["d1-b01", "d1-b02", "d1-b03", "d1-b04", "d1-b05", "d1-b06", "d1-b07"]
    return _doc("doc_single_zh", boxes, reference, ["single_column", "header_footer"])


def _doc_double_en(rng: random.Random) -> dict[str, Any]:
    boxes = [
        _box("d2-b01", 0.10, 0.020, 0.90, 0.044, "small", "Journal of Document Engineering Vol.9", "en"),
        _box("d2-b02", 0.10, 0.075, 0.90, 0.115, "xlarge", "Two-Column Reading Order Recovery", "en"),
    ]
    y = 0.155
    for idx in range(4):
        top = round(y + _jitter(rng, 0.001), 4)
        bottom = round(top + 0.128, 4)
        boxes.append(_box(f"d2-l{idx + 1}", 0.10, top, 0.47, bottom, "body",
                          _EN_SNIPPETS[idx], "en"))
        boxes.append(_box(f"d2-r{idx + 1}", 0.53, top, 0.90, bottom, "body",
                          _EN_SNIPPETS[idx + 4], "en"))
        y = round(y + 0.172, 4)
    boxes.append(_box("d2-b10", 0.10, 0.945, 0.90, 0.966, "xsmall", "Page 1 of 1", "en"))
    reference = [
        "d2-b01", "d2-b02",
        "d2-l1", "d2-l2", "d2-l3", "d2-l4",
        "d2-r1", "d2-r2", "d2-r3", "d2-r4",
        "d2-b10",
    ]
    return _doc("doc_double_en", boxes, reference, ["double_column", "spanning_title", "header_footer"])


def _doc_double_mixed(rng: random.Random) -> dict[str, Any]:
    boxes = [
        _box("d3-b01", 0.10, 0.020, 0.90, 0.044, "small", "中英混排期刊 Bilingual Journal 第 7 期", "mixed"),
        _box("d3-b02", 0.10, 0.075, 0.90, 0.115, "xlarge", "双栏通栏标题 Spanning Heading 示例", "mixed"),
    ]
    rows = [
        (0.150, 0.265),
        (0.285, 0.400),
    ]
    seq = 0
    for top0, bottom0 in rows:
        seq += 1
        top = round(top0 + _jitter(rng, 0.001), 4)
        bottom = round(bottom0 + _jitter(rng, 0.001), 4)
        boxes.append(_box(f"d3-l{seq}", 0.10, top, 0.47, bottom, "body",
                          _ZH_SNIPPETS[seq - 1], "zh"))
        boxes.append(_box(f"d3-r{seq}", 0.53, top, 0.90, bottom, "body",
                          _EN_SNIPPETS[seq - 1], "en"))
    boxes.append(_box("d3-s02", 0.10, 0.430, 0.90, 0.462, "large",
                      "通栏小节标题 Section Heading 跨两栏", "mixed"))
    rows2 = [
        (0.490, 0.605),
        (0.625, 0.740),
    ]
    for top0, bottom0 in rows2:
        seq += 1
        top = round(top0 + _jitter(rng, 0.001), 4)
        bottom = round(bottom0 + _jitter(rng, 0.001), 4)
        boxes.append(_box(f"d3-l{seq}", 0.10, top, 0.47, bottom, "body",
                          _MIXED_SNIPPETS[seq - 3], "mixed"))
        boxes.append(_box(f"d3-r{seq}", 0.53, top, 0.90, bottom, "body",
                          _MIXED_SNIPPETS[seq - 2], "mixed"))
    boxes.append(_box("d3-b11", 0.10, 0.945, 0.90, 0.966, "xsmall", "第 1 页 Page 1", "mixed"))
    reference = [
        "d3-b01", "d3-b02",
        "d3-l1", "d3-l2", "d3-r1", "d3-r2",
        "d3-s02",
        "d3-l3", "d3-l4", "d3-r3", "d3-r4",
        "d3-b11",
    ]
    return _doc(
        "doc_double_mixed",
        boxes,
        reference,
        ["double_column", "spanning_title", "mid_spanning_heading", "header_footer", "zh_en_mixed"],
    )


def _doc_single_mixed_fragments(rng: random.Random) -> dict[str, Any]:
    boxes = [
        _box("d4-b01", 0.10, 0.020, 0.90, 0.044, "small", "混排简报 Mixed Bulletin 第 12 期", "mixed"),
        _box("d4-b02", 0.10, 0.080, 0.90, 0.118, "xlarge", "碎片框合并与单栏排序", "zh"),
        _box("d4-b03", 0.10, 0.160, 0.90, 0.235, "body",
             "本框为完整段落，不参与合并；" + _ZH_SNIPPETS[2], "zh"),
        # 同一阅读行上的两个相邻碎片，归一化水平间隙 0.01（约 8px < 12px 阈值）
        _box("d4-b04a", 0.10, 0.270, 0.50, 0.312, "body",
              "同一行被切分为两个碎片 fragment A，", "zh"),
        _box("d4-b04b", 0.51, 0.270, 0.90, 0.312, "body",
              "fragment B should merge with A.", "en"),
        _box("d4-b05a", 0.10, 0.345, 0.46, 0.387, "body",
              "第二行碎片 left piece，", "zh"),
        _box("d4-b05b", 0.47, 0.345, 0.90, 0.387, "body",
              "right piece 合并 gap below threshold.", "en"),
        _box("d4-b06", 0.10, 0.420, 0.90, 0.500, "body",
              "Merged blocks keep deterministic ids and byte-stable output. "
              + _EN_SNIPPETS[5], "en"),
        _box("d4-b07", 0.10, 0.535, 0.90, 0.615, "body",
              _MIXED_SNIPPETS[0] + "；" + _MIXED_SNIPPETS[1], "mixed"),
        _box("d4-b08", 0.10, 0.945, 0.90, 0.966, "xsmall", "第 1 页 Page 1", "mixed"),
    ]
    reference = [
        "d4-b01", "d4-b02", "d4-b03",
        "d4-b04a", "d4-b04b",
        "d4-b05a", "d4-b05b",
        "d4-b06", "d4-b07", "d4-b08",
    ]
    return _doc(
        "doc_single_mixed_fragments",
        boxes,
        reference,
        ["single_column", "fragment_merge", "header_footer", "zh_en_mixed"],
    )


def _doc(doc_id: str, boxes: list[dict[str, Any]], reference: list[str],
         tags: list[str]) -> dict[str, Any]:
    box_ids = [box["box_id"] for box in boxes]
    if len(set(box_ids)) != len(box_ids):
        raise ValueError(f"{doc_id} 存在重复 box_id")
    if sorted(reference) != sorted(box_ids):
        raise ValueError(f"{doc_id} 参考顺序与框集合不一致")
    return {
        "doc_id": doc_id,
        "page_width": PAGE_W,
        "page_height": PAGE_H,
        "tags": tags,
        "boxes": boxes,
        "reference_order": reference,
    }


def generate_documents(seed: int = SEED) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    docs = [
        _doc_single_zh(rng),
        _doc_double_en(rng),
        _doc_double_mixed(rng),
        _doc_single_mixed_fragments(rng),
    ]
    # 固定输出顺序：按 doc_id 排序，避免依赖生成顺序
    return sorted(docs, key=lambda doc: doc["doc_id"])
