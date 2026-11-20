#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import random
import shutil
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from xrel_workbench.config import default_config, validate_config  # noqa: E402
from xrel_workbench.util import (  # noqa: E402
    atomic_write_json,
    atomic_write_text,
    digest_file,
    jsonl_bytes,
    write_jsonl,
)


DEFAULT_SEED = 20261121


def bridge_dictionary() -> dict[str, Any]:
    raw_entries = [
        ("br-001", "跨语", ["cross", "language"], 0.95),
        ("br-002", "语言", ["language"], 0.90),
        ("br-003", "检索", ["search", "retrieval"], 0.92),
        ("br-004", "信息", ["information"], 0.90),
        ("br-005", "数据", ["data"], 0.88),
        ("br-006", "算法", ["algorithm"], 0.88),
        ("br-007", "平台", ["platform"], 0.82),
        ("br-008", "模型", ["model"], 0.90),
        ("br-009", "评估", ["evaluation", "evaluate", "assessment"], 0.86),
        ("br-010", "准确", ["accuracy", "accurate"], 0.84),
        ("br-011", "风控", ["risk", "control"], 0.83),
        ("br-012", "风险", ["risk"], 0.83),
        ("br-013", "控制", ["control"], 0.82),
        ("br-014", "天气", ["weather"], 0.90),
        ("br-015", "北京", ["beijing"], 0.95),
        ("br-016", "预报", ["forecast"], 0.88),
        ("br-017", "图书", ["book", "library"], 0.86),
        ("br-018", "股票", ["stock"], 0.85),
        ("br-019", "市场", ["market"], 0.85),
        ("br-020", "分析", ["analysis", "analyze"], 0.82),
        ("br-021", "系统", ["system"], 0.80),
        ("br-022", "监控", ["monitoring", "monitor"], 0.80),
        ("br-023", "离线", ["offline"], 0.80),
        ("br-024", "空白", ["blank", "empty"], 0.80),
        ("br-025", "查询", ["query"], 0.90),
        ("br-026", "率", ["rate"], 0.72),
        ("br-027", "背景", ["background"], 0.75),
        ("br-028", "文本", ["text"], 0.82),
    ]
    return {
        "format_version": 1,
        "name": "deterministic-demo-bridge",
        "entries": [
            {"id": entry_id, "zh": zh, "en": en, "weight": weight}
            for entry_id, zh, en, weight in raw_entries
        ],
    }


def _pair(
    pair_id: str,
    query: str,
    doc: str,
    lang_query: str,
    lang_doc: str,
    source: str,
    human_relevance: int | None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "pair_id": pair_id,
        "query": query,
        "doc": doc,
        "lang_query": lang_query,
        "lang_doc": lang_doc,
        "source": source,
    }
    if human_relevance is not None:
        value["human_relevance"] = human_relevance
    return value


def demo_shards(seed: int) -> dict[str, list[dict[str, Any]]]:
    rng = random.Random(seed)
    filler = ["背景文本", "样本记录", "说明内容", "附加段落", "检索日志", "测试语料"]
    long_zh = "模型评估 " + " ".join(rng.choice(filler) for _ in range(110)) + " 模型评估"
    long_en = (
        "algorithm platform data service "
        + " ".join(rng.choice(["background text", "sample record", "system log"]) for _ in range(90))
        + " algorithm platform"
    )
    duplicate_query = "跨语言检索"
    duplicate_doc = "cross language retrieval for enterprise search"

    shard1 = [
        _pair("p0001", duplicate_query, duplicate_doc, "zh", "en", "synthetic_cross_zh_en", 3),
        _pair("p0002", "information retrieval accuracy", "信息检索的准确评估", "en", "zh", "synthetic_cross_en_zh", 3),
        _pair("p0003", "风控模型评估", "风控模型离线评估与监控", "zh", "zh", "synthetic_same_zh", 3),
        _pair("p0004", "library book search", "library book search and catalog", "en", "en", "synthetic_same_en", 3),
        _pair("p0005", "beijing weather forecast", "北京天气预报", "en", "zh", "synthetic_cross_en_zh", 3),
        _pair("p0006", "数据算法平台", "data algorithm platform", "zh", "en", "synthetic_cross_zh_en", 2),
        _pair("p0007", duplicate_query, duplicate_doc, "zh", "en", "synthetic_duplicate_across_shards", 3),
        _pair("p0008", "", "stock market report", "zh", "en", "synthetic_empty_query", 0),
    ]
    shard2 = [
        _pair("p0009", duplicate_query, duplicate_doc, "zh", "en", "synthetic_duplicate_across_shards", 3),
        _pair("p0010", "信息检索准确评估", "information retrieval accuracy evaluation", "zh", "en", "synthetic_cross_zh_en", 3),
        _pair("p0011", "risk control model", "risk control model monitoring", "en", "en", "synthetic_same_en", 3),
        _pair("p0012", "图书检索", "图书检索系统", "zh", "zh", "synthetic_same_zh", 3),
        _pair("p0013", "北京天气预报", "weather forecast for beijing", "zh", "en", "synthetic_cross_zh_en", 3),
        _pair("p0014", "data algorithm platform", "数据算法平台", "en", "zh", "synthetic_cross_en_zh", 3),
        _pair("p0015", "股票市场分析", "weather forecast for beijing", "zh", "en", "synthetic_low_relevance", 0),
        _pair("p0016", "model evaluation", long_zh, "en", "zh", "synthetic_long_doc", 2),
    ]
    shard3 = [
        _pair("p0017", "跨语言模型评估", "cross language model evaluation", "zh", "en", "synthetic_cross_zh_en", 3),
        _pair("p0018", "search accuracy", "检索准确率", "en", "zh", "synthetic_cross_en_zh", 3),
        _pair("p0019", "天气北京", "北京天气预报", "zh", "zh", "synthetic_same_zh", 2),
        _pair("p0020", "stock market analysis", "stock market analysis report", "en", "en", "synthetic_same_en", 3),
        _pair("p0021", "风险控制", "risk control", "zh", "en", "synthetic_cross_zh_en", 3),
        _pair("p0022", "library book search", "图书检索系统", "en", "zh", "synthetic_human_offset", 1),
        _pair("p0023", "   ", "", "zh", "zh", "synthetic_empty_query_and_doc", 0),
        _pair("p0024", "algorithm platform", long_en, "en", "en", "synthetic_long_doc", 2),
    ]
    return {"part-001": shard1, "part-002": shard2, "part-003": shard3}


def generate(output: Path, seed: int, *, force: bool = False) -> dict[str, Any]:
    output = output.resolve()
    if output.exists() and any(output.iterdir()) and not force:
        raise SystemExit(f"output directory is not empty: {output}; use --force")
    if output.exists() and force:
        shutil.rmtree(output)
    shard_dir = output / "shards"
    shard_dir.mkdir(parents=True, exist_ok=True)
    shards = demo_shards(seed)
    shard_summary: dict[str, Any] = {}
    for name, records in shards.items():
        path = shard_dir / f"{name}.jsonl"
        write_jsonl(path, records)
        shard_summary[name] = {
            "pair_count": len(records),
            "digest": digest_file(path),
        }
    bridge = bridge_dictionary()
    bridge_path = output / "bridge.json"
    atomic_write_json(bridge_path, bridge, indent=2)
    config = copy.deepcopy(default_config())
    config["version"] = 3
    validate_config(config)
    scoring_path = output / "scoring.yaml"
    atomic_write_text(
        scoring_path,
        yaml.safe_dump(config, allow_unicode=True, sort_keys=True, default_flow_style=False),
    )
    manifest = {
        "format_version": 1,
        "generator": "scripts/generate_demo.py",
        "seed": seed,
        "shards": shard_summary,
        "bridge": {"entry_count": len(bridge["entries"]), "digest": digest_file(bridge_path)},
        "scoring": {"digest": digest_file(scoring_path)},
        "coverage": {
            "cross_zh_en": True,
            "cross_en_zh": True,
            "same_language": True,
            "duplicate_across_shards": True,
            "long_text": True,
            "empty_query": True,
            "empty_doc": True,
        },
    }
    atomic_write_json(output / "manifest.json", manifest, indent=2)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic cross-lingual demo data")
    parser.add_argument("--out", type=Path, default=ROOT / "data")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    manifest = generate(args.out, args.seed, force=args.force)
    print(json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
