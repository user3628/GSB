"""阶段常量与可配置参数。

配置按阶段分组，版本指纹按阶段取用对应子集，从而做到某一参数变化时
只令真正受影响的最早阶段及其下游失效。
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

STAGE_NORMALIZE = "normalize"
STAGE_COLUMN = "column"
STAGE_ORDER = "order"
STAGE_SUMMARY = "summary"
STAGES: tuple[str, ...] = (STAGE_NORMALIZE, STAGE_COLUMN, STAGE_ORDER, STAGE_SUMMARY)

# 各阶段序号（单调序号 seq 即该值；0 保留给原始输入记录）
STAGE_SEQ: dict[str, int] = {name: idx + 1 for idx, name in enumerate(STAGES)}

DEFAULT_CONFIG: dict[str, Any] = {
    "config_version": "cfg-2026-11-21-v1",
    "normalize": {
        # 像素坐标保留小数位（归一化坐标换算后仍保持确定精度）
        "coord_decimals": 3,
        # 合并：水平/垂直间隙（像素）小于该阈值且阅读方向相邻则合并
        "merge_gap_px": 12.0,
        # 两框垂直重叠占较短框高度比例低于该值时不视为同一阅读行
        "merge_overlap_ratio": 0.5,
        # 页眉/页脚：距最近页边（归一化）小于该阈值
        "header_footer_margin": 0.08,
        # 页眉/页脚仅接受这些小字号档位
        "header_footer_font_tiers": ["small", "xsmall"],
    },
    "column": {
        # 候选栏框宽度占有效版面宽度比例高于该值即视为通栏
        "spanning_width_ratio": 0.80,
        # 中位框宽占有效版面宽度比例高于该值判为单栏
        "single_column_ratio": 0.75,
        # 双栏聚类：两簇水平起点间隔需大于有效宽度的该比例
        "cluster_gap_ratio": 0.12,
        # 直方图分箱数（参与判定依据记录）
        "histogram_bins": 8,
    },
    "order": {
        # 跨栏（通栏）块裁决优先级：span_first 或 column_first 或 flow
        "span_priority": "flow",
        # 并列块裁决：同列按 top，跨列按列序
        "side_by_side_priority": "top_then_column",
        # 顺序置信度权重，和应为 1.0
        "confidence_weights": {
            "vertical_gap": 0.6,
            "column_membership": 0.4,
        },
        # 垂直间隙归一化参考（像素），超过该值间隙得分记 0
        "gap_reference_px": 80.0,
    },
}


def validate_config(config: dict[str, Any]) -> None:
    weights = config["order"]["confidence_weights"]
    total = sum(weights.values())
    if abs(total - 1.0) > 1e-9:
        raise ValueError(f"confidence_weights 之和必须为 1.0，当前为 {total}")
    if config["order"]["span_priority"] not in {"flow", "span_first", "column_first"}:
        raise ValueError("span_priority 必须为 flow/span_first/column_first")


def stage_config(config: dict[str, Any], stage: str) -> dict[str, Any]:
    """返回某阶段相关的配置子集（含全局 config_version）。

    summary 阶段无独立参数，其指纹由输入内容与全局配置版本共同决定。
    """
    return {
        "config_version": config["config_version"],
        stage: deepcopy(config.get(stage, {})),
    }


def merge_config(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    config = deepcopy(DEFAULT_CONFIG)
    if not overrides:
        return config
    for group, values in overrides.items():
        if group == "config_version":
            config["config_version"] = values
        elif group in config and isinstance(values, dict):
            config[group].update(values)
        else:
            raise ValueError(f"未知配置组: {group}")
    validate_config(config)
    return config
