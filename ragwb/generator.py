"""Deterministic local demo-data generator.

Everything is produced from a fixed :class:`random.Random` seed and serialized
with the canonical JSON helpers, so regenerating the demo data yields
byte-identical shards and configuration.  No network or model is involved.
"""
from __future__ import annotations

import os
import random
from typing import Any, Dict, List

from . import io_utils, yaml_lite

SEED = 20261123
CONFIG_VERSION = "1.0.0"

# Hand-curated knowledge-base paragraphs.  Multiple paragraphs describe the
# same fact with agreeing or conflicting numbers/dates, channels and language
# tags so alignment and conflict adjudication have real material to work with.
CURATED_DOCS: List[Dict[str, Any]] = [
    {"doc_id": "kb-0001", "version": 3, "channel": "official", "lang": "zh",
     "text": "极光电池容量为 75 千瓦时，发布日期 2024-03-12。"},
    {"doc_id": "kb-0002", "version": 2, "channel": "report", "lang": "zh",
     "text": "极光电池容量为 70 千瓦时，发布日期 2024-01-05。"},
    {"doc_id": "kb-0003", "version": 1, "channel": "wiki", "lang": "en",
     "text": "Aurora battery capacity is 75 kWh, announced on 2024-03-12."},
    {"doc_id": "kb-0004", "version": 1, "channel": "forum", "lang": "zh",
     "text": "极光电池容量高达 90 千瓦时，属于网友传闻。"},
    {"doc_id": "kb-0005", "version": 2, "channel": "official", "lang": "zh",
     "text": "星河芯片主频为 3.2 GHz，量产时间 2025-06-01。"},
    {"doc_id": "kb-0006", "version": 3, "channel": "report", "lang": "zh",
     "text": "星河芯片主频为 3.5 GHz，量产时间 2025-09-18。"},
    {"doc_id": "kb-0007", "version": 2, "channel": "forum", "lang": "zh",
     "text": "星河芯片主频维持 3.2 GHz，与官方参数一致。"},
    {"doc_id": "kb-0008", "version": 2, "channel": "official", "lang": "en",
     "text": "The Nimbus drone has a flight time of 40 minutes and was "
             "released on 2024-11-02."},
    {"doc_id": "kb-0009", "version": 1, "channel": "wiki", "lang": "en",
     "text": "Nimbus drone flight time is 34 minutes according to early "
             "speculation."},
    {"doc_id": "kb-0010", "version": 1, "channel": "report", "lang": "zh",
     "text": "Nimbus 无人机续航 40 分钟，发布日期 2024-11-02。"},
    {"doc_id": "kb-0011", "version": 4, "channel": "official", "lang": "zh",
     "text": "云岭手机售价 3999 元，上市日期 2025-02-20。"},
    {"doc_id": "kb-0012", "version": 2, "channel": "report", "lang": "zh",
     "text": "云岭手机售价 4299 元，上市日期 2025-01-10。"},
    {"doc_id": "kb-0013", "version": 3, "channel": "forum", "lang": "zh",
     "text": "云岭手机售价 3999 元，论坛团购价同步。"},
]

FILLER_ENTITIES = ["晨风", "远山", "海潮", "岩湾", "松林"]
FILLER_NOUNS = ["路由器", "显示器", "键盘", "充电器", "传感器"]

PRONOUNS_ZH = ["它", "该条", "这条", "其"]


def _span(record: Dict[str, Any], text: str) -> Dict[str, Any]:
    start = record["question"].find(text)
    return {"start": start, "end": start + len(text), "text": text,
            "kind": "pronoun"}


def _build_sessions() -> Dict[str, List[Dict[str, Any]]]:
    sessions: Dict[str, List[Dict[str, Any]]] = {}

    s1: List[Dict[str, Any]] = [
        {"turn_index": 0, "lang": "zh", "focus_hint": "极光电池",
         "question": "极光电池的容量是多少？", "coref_spans": []},
        {"turn_index": 1, "lang": "zh", "focus_hint": None,
         "question": "它是什么时候发布的？",
         "coref_spans": []},
        {"turn_index": 2, "lang": "zh", "focus_hint": None,
         "question": "该条信息的来源通道是什么？",
         "coref_spans": []},
    ]
    s1[1]["coref_spans"] = [_span(s1[1], "它")]
    s1[2]["coref_spans"] = [_span(s1[2], "该条")]
    sessions["sess-s1-aurora"] = s1

    s2: List[Dict[str, Any]] = [
        {"turn_index": 0, "lang": "zh", "focus_hint": "星河芯片",
         "question": "星河芯片的主频是多少？", "coref_spans": []},
        {"turn_index": 1, "lang": "zh", "focus_hint": None,
         "question": "它的量产时间呢？", "coref_spans": []},
        {"turn_index": 2, "lang": "zh", "focus_hint": "云岭手机",
         "question": "云岭手机售价是多少？", "coref_spans": []},
        {"turn_index": 3, "lang": "zh", "focus_hint": None,
         "question": "它什么时候上市？", "coref_spans": []},
    ]
    s2[1]["coref_spans"] = [_span(s2[1], "它")]
    s2[3]["coref_spans"] = [_span(s2[3], "它")]
    sessions["sess-s2-conflict"] = s2

    s3: List[Dict[str, Any]] = [
        {"turn_index": 0, "lang": "en",
         "focus_hint": "Nimbus drone",
         "question": "What is the flight time of the Nimbus drone?",
         "coref_spans": []},
        {"turn_index": 1, "lang": "zh", "focus_hint": None,
         "question": "它的发布日期是什么时候？", "coref_spans": []},
        {"turn_index": 2, "lang": "mixed", "focus_hint": None,
         "question": "Nimbus 的续航是多少？", "coref_spans": []},
        {"turn_index": 3, "lang": "en", "focus_hint": None,
         "question": "When was it released?", "coref_spans": []},
    ]
    s3[1]["coref_spans"] = [_span(s3[1], "它")]
    s3[3]["coref_spans"] = [_span(s3[3], "it")]
    sessions["sess-s3-bilingual"] = s3

    rng = random.Random(SEED)
    s4: List[Dict[str, Any]] = []
    broad_questions = [
        "请介绍全部产品的版本信息？",
        "它还有哪些公开参数？",
        "这条信息与其他产品相比如何？",
        "全部产品的发布节奏是什么？",
        "它的同系列产品有哪些？",
        "该条之外还有什么补充信息？",
        "汇总所有产品的版本与通道？",
        "它们之间有什么共性？",
    ]
    for index, question in enumerate(broad_questions):
        record = {
            "turn_index": index, "lang": "zh",
            "focus_hint": "产品信息" if index == 0 else None,
            "question": question, "coref_spans": [],
        }
        for pronoun in PRONOUNS_ZH + ["它们"]:
            if pronoun in question:
                record["coref_spans"] = [_span(record, pronoun)]
                break
        s4.append(record)
    sessions["sess-s4-budget"] = s4

    s5 = [
        {"turn_index": 0, "lang": "zh", "focus_hint": None,
         "question": "它的价格是多少？", "coref_spans": []},
    ]
    s5[0]["coref_spans"] = [_span(s5[0], "它")]
    sessions["sess-s5-fallback"] = s5

    # Filler knowledge-base paragraphs (deterministically generated).  They
    # intentionally share generic vocabulary so the budget-overflow session
    # accumulates real evaluation counts.
    filler_docs: List[Dict[str, Any]] = []
    channels = ["official", "report", "wiki", "forum"]
    for index in range(11):
        number = 1000 + index * 7
        entity = FILLER_ENTITIES[index % len(FILLER_ENTITIES)]
        noun = FILLER_NOUNS[index % len(FILLER_NOUNS)]
        year = 2023 + (index % 3)
        channel = channels[index % len(channels)]
        version = 1 + (index % 4)
        filler_docs.append({
            "doc_id": f"kb-{index + 14:04d}", "version": version,
            "channel": channel,
            "lang": "en" if index % 4 == 1 else "zh",
            "text": (f"{entity}{noun}产品版本 {version} 公开参数编号 "
                     f"{number}，发布日期 {year}-05-{(index % 9) + 1:02d}，"
                     f"属于常规产品信息。"),
        })
    docs = list(CURATED_DOCS) + filler_docs
    assert len(docs) == 24
    # Deterministic shuffle to prove ordering never affects results.
    rng.shuffle(docs)
    return sessions, docs


def _build_config() -> Dict[str, Any]:
    return {
        "config_version": CONFIG_VERSION,
        "seed": SEED,
        "retrieval": {
            "depth": 8,
            "evidence_limit": 5,
            "alignment_tolerance": 0.35,
            "channel_prior": {
                "official": 1.0,
                "report": 0.6,
                "wiki": 0.2,
                "forum": -0.3,
            },
            "weights": {
                "overlap": 1.0,
                "freshness": 0.15,
                "channel": 0.8,
                "credibility_version": 0.2,
                "credibility_channel": 1.0,
                "credibility_language": 0.5,
            },
        },
        "answer": {
            "template": "答案：{answer_body}\n引用：{references}",
            "required_slots": ["answer_body", "references"],
        },
        "scale_tiers": {
            "small": {"max_paragraphs": 50, "max_turns": 6,
                      "strategy": "exact_full"},
            "medium": {"max_paragraphs": 200, "max_turns": 20,
                       "strategy": "layered_truncate",
                       "truncate_depth": 40},
            "large": {"above_paragraphs": 200,
                      "strategy": "focus_first", "truncate_depth": 12},
        },
        "budget": {"global_evidence_evaluations": 300},
    }


def generate(data_dir: str) -> Dict[str, str]:
    """Write shards and configuration; return a manifest of output paths."""
    sessions, docs = _build_sessions()
    session_dir = os.path.join(data_dir, "sessions")
    kb_dir = os.path.join(data_dir, "kb")
    io_utils.ensure_dir(session_dir)
    io_utils.ensure_dir(kb_dir)

    outputs: Dict[str, str] = {}
    for session_id, turns in sorted(sessions.items()):
        rows = []
        for turn in turns:
            rows.append({
                "session_id": session_id,
                "turn_index": turn["turn_index"],
                "question": turn["question"],
                "lang": turn["lang"],
                "focus_hint": turn["focus_hint"],
                "coref_spans": turn["coref_spans"],
            })
        path = os.path.join(session_dir, f"{session_id}.jsonl")
        io_utils.write_jsonl(path, rows)
        outputs[session_id] = path

    # Split the 24 paragraphs across three knowledge-base shards.
    shard_size = (len(docs) + 2) // 3
    shard_paths: List[str] = []
    for shard_index in range(3):
        chunk = docs[shard_index * shard_size:(shard_index + 1) * shard_size]
        if not chunk:
            continue
        path = os.path.join(kb_dir, f"kb_shard_{shard_index + 1}.jsonl")
        io_utils.write_jsonl(path, chunk)
        shard_paths.append(path)
    outputs["kb_shards"] = ";".join(shard_paths)  # type: ignore[assignment]

    config_path = os.path.join(data_dir, "config.yaml")
    yaml_lite.dump(config_path, _build_config())
    outputs["config"] = config_path

    manifest = {
        "seed": SEED,
        "config_version": CONFIG_VERSION,
        "session_order": [
            "sess-s1-aurora",
            "sess-s2-conflict",
            "sess-s3-bilingual",
            "sess-s5-fallback",
            "sess-s4-budget",
        ],
        "session_files": sorted(
            os.path.relpath(v, data_dir)
            for k, v in outputs.items()
            if k.startswith("sess-")
        ),
        "kb_shards": [
            os.path.relpath(path, data_dir) for path in shard_paths
        ],
        "config_path": os.path.relpath(config_path, data_dir),
        "paragraph_count": len(docs),
        "session_count": len(sessions),
    }
    manifest_path = os.path.join(data_dir, "manifest.json")
    io_utils.write_json(manifest_path, manifest, pretty=True)
    outputs["manifest"] = manifest_path
    return outputs
