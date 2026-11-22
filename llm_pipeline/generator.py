"""Deterministic local demo data generator (no network access)."""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

from .config import default_config, validate_config
from .util import atomic_write_bytes, canonical_json_line, sha256_bytes, sha256_obj


SEED = 20261123

# Deliberately hand-authored short passages so approximate-duplicate behaviour
# is stable across Python and library upgrades.
_WEB_ZH = [
    "人工智能正在改变搜索引擎，模型需要高质量的中文预训练语料。",
    "北京今天的天气晴朗，适合到公园散步并阅读一本好书。",
    "机器学习系统依赖可靠的数据管线，增量更新可以减少重复计算。",
    "开源社区通过协作改进软件质量，测试与文档同样重要。",
    "海量文本经过规范化与近似去重后，可以形成更均衡的训练集合。",
    "长江流经多个省份，沿岸城市拥有不同的历史与文化。",
    "数据治理需要记录来源、语言和版本，确保结果可以复现。",
    "分布式系统在并发发布时必须使用锁和原子写入保护状态。",
]
_NEWS_ZH = [
    "国务院发布新一轮公共服务建设计划，重点关注基层医疗与教育。",
    "上海港今年集装箱吞吐量保持增长，航运企业加快数字化改造。",
    "科研团队公布新型储能材料进展，相关实验数据已经开放。",
    "新能源汽车市场持续扩大，充电基础设施覆盖更多城市。",
    "城市博物馆推出夜间展览，吸引年轻观众了解地方历史。",
]
_WEB_EN = [
    "Large language models require diverse and carefully filtered pretraining data.",
    "Deterministic pipelines make experiments reproducible across machines.",
    "Approximate duplicate detection reduces wasted computation and memorization.",
    "The city library opens early and closes late during the summer.",
    "Version fingerprints identify the earliest stage affected by a configuration change.",
    "Atomic file replacement prevents readers from observing half written checkpoints.",
]
_NEWS_EN = [
    "Stratified sampling keeps source and language weights under control.",
    "Coastal cities are investing in resilient public transport networks.",
    "Researchers published an open benchmark for multilingual retrieval systems.",
    "Electric vehicle sales grew while charging networks expanded nationwide.",
]
_FORUM_MIXED = [
    "LLM 训练需要 balanced 数据配比，同时保留多语言 samples。",
    "SimHash 通过 64-bit 指纹识别近似 duplicates，并降低 storage 成本。",
    "增量 pipeline 只重建 invalid stages，upstream artifacts 可以继续复用。",
    "检查点 checkpoint 使用 monotonic sequence，避免 concurrent publish 冲突。",
]


def _record(sample_id: str, source: str, lang: str, text: str) -> dict[str, Any]:
    return {"id": sample_id, "source": source, "lang": lang, "text": text}


def demo_records() -> dict[str, list[dict[str, Any]]]:
    """Return the three deterministic shards as an ordered mapping."""

    shard_1: list[dict[str, Any]] = []
    shard_2: list[dict[str, Any]] = []
    shard_3: list[dict[str, Any]] = []

    for index, text in enumerate(_WEB_ZH, start=1):
        target = shard_1 if index <= 5 else shard_2
        target.append(_record(f"demo-{index:04d}", "web", "zh", text))
    for index, text in enumerate(_NEWS_ZH, start=9):
        target = shard_1 if index <= 11 else shard_2
        target.append(_record(f"demo-{index:04d}", "news", "zh", text))
    for index, text in enumerate(_WEB_EN, start=14):
        target = shard_1 if index <= 15 else shard_2 if index <= 17 else shard_3
        target.append(_record(f"demo-{index:04d}", "web", "en", text))
    for index, text in enumerate(_NEWS_EN, start=20):
        target = shard_2 if index <= 21 else shard_3
        target.append(_record(f"demo-{index:04d}", "news", "en", text))
    for index, text in enumerate(_FORUM_MIXED, start=24):
        target = shard_2 if index <= 25 else shard_3
        target.append(_record(f"demo-{index:04d}", "forum", "mixed", text))

    # Cross-shard approximate duplicates (lowest id is the deterministic keeper).
    shard_2.append(
        _record(
            "demo-0031",
            "web",
            "zh",
            "人工智能正在改变搜索引擎,模型需要高质量的中文预训练语料!",
        )
    )
    shard_3.append(
        _record(
            "demo-0032",
            "web",
            "en",
            "Ｌａｒｇｅ language models require diverse and carefully filtered pretraining data.",
        )
    )
    shard_2.append(
        _record(
            "demo-0033",
            "web",
            "en",
            "Deterministic pipelines make experiment reproducible across machines.",
        )
    )
    shard_3.append(
        _record(
            "demo-0034",
            "forum",
            "mixed",
            "LLM 训练需要 balanced 数据配比,同时保留多语言 samples!",
        )
    )
    shard_2.append(
        _record(
            "demo-0035",
            "news",
            "en",
            "Stratified sampling keeps source and language weights under control.",
        )
    )

    # Empty and whitespace-only anomalies, including a layer with no kept rows.
    shard_1.append(_record("demo-0091", "archive", "zh", ""))
    shard_2.append(_record("demo-0092", "archive", "zh", "   \n\t"))
    shard_3.append(_record("demo-0093", "archive", "zh", " \u3000 "))

    # Use the fixed seed to drive construction order.  The final records are
    # sorted by id, so the seed controls generation while byte stability does
    # not depend on filesystem or dictionary iteration order.
    plan: list[tuple[str, dict[str, Any]]] = []
    for shard_name, shard_rows in (
        ("shard-001.jsonl", shard_1),
        ("shard-002.jsonl", shard_2),
        ("shard-003.jsonl", shard_3),
    ):
        plan.extend((shard_name, row) for row in shard_rows)
    random.Random(SEED).shuffle(plan)
    shard_by_name = {
        "shard-001.jsonl": shard_1,
        "shard-002.jsonl": shard_2,
        "shard-003.jsonl": shard_3,
    }
    for shard_rows in shard_by_name.values():
        shard_rows.clear()
    for shard_name, row in plan:
        shard_by_name[shard_name].append(row)

    # Make the files themselves stable and human-inspectable.
    for shard in (shard_1, shard_2, shard_3):
        shard.sort(key=lambda row: row["id"])
    return {
        "shard-001.jsonl": shard_1,
        "shard-002.jsonl": shard_2,
        "shard-003.jsonl": shard_3,
    }


def demo_config() -> dict[str, Any]:
    config = default_config()
    config["allocation"]["target_samples"] = 24
    config["allocation"]["weights"] = {
        "web::zh": 8,
        "web::en": 5,
        "news::zh": 4,
        "news::en": 3,
        "forum::mixed": 4,
        # Configured but intentionally empty in the generated shards.
        "forum::en": 2,
        # Observed only through empty-text anomalies and therefore empty after dedup.
        "archive::zh": 1,
    }
    config["dedup"]["hamming_threshold"] = 7
    return validate_config(config)


def generate_demo_assets(output_dir: str | Path) -> dict[str, Any]:
    output = Path(output_dir)
    shards_dir = output / "shards"
    shards_dir.mkdir(parents=True, exist_ok=True)
    config = demo_config()
    shards = demo_records()
    shard_manifest: list[dict[str, Any]] = []
    for name in sorted(shards):
        raw = b"".join(canonical_json_line(row) for row in shards[name])
        path = shards_dir / name
        atomic_write_bytes(path, raw)
        shard_manifest.append(
            {
                "name": name,
                "digest": sha256_bytes(raw),
                "record_count": len(shards[name]),
            }
        )
    atomic_write_bytes(
        output / "config.json",
        canonical_json_line(config),
    )
    manifest = {
        "seed": SEED,
        "generator": "llm_pipeline.generator.v1",
        "config_digest": sha256_obj(config),
        "shards": shard_manifest,
        "config": config,
    }
    atomic_write_bytes(output / "manifest.json", canonical_json_line(manifest))
    return manifest


def write_demo_without_overwrite(output_dir: str | Path) -> dict[str, Any]:
    return generate_demo_assets(output_dir)


if __name__ == "__main__":  # pragma: no cover - convenience entry point
    print(json.dumps(generate_demo_assets("demo"), ensure_ascii=False, indent=2, sort_keys=True))
