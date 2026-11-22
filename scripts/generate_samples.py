#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any

import jsonlines


def _record(
    sample_id: str,
    *,
    prompt: str,
    model_output: str,
    reference_answer: str,
    retrieved_evidence: list[str],
    lang: str,
    source: str,
    annotation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "sample_id": sample_id,
        "prompt": prompt,
        "model_output": model_output,
        "reference_answer": reference_answer,
        "retrieved_evidence": retrieved_evidence,
        "lang": lang,
        "source": source,
    }
    if annotation is not None:
        record["annotation"] = annotation
    return record


def build_records() -> list[dict[str, Any]]:
    records = [
        _record(
            "zh-001",
            prompt="法国的首都是哪里？",
            model_output="法国的首都是巴黎。",
            reference_answer="巴黎",
            retrieved_evidence=["法国的首都是巴黎。"],
            lang="zh",
            source="kb_zh",
            annotation={"is_hallucination": False, "needs_review": False},
        ),
        _record(
            "zh-002",
            prompt="法国的首都是哪里？",
            model_output="法国的首都是里昂。",
            reference_answer="巴黎",
            retrieved_evidence=["法国的首都是巴黎。"],
            lang="zh",
            source="kb_zh",
            annotation={"is_hallucination": True, "needs_review": False},
        ),
        _record(
            "zh-003",
            prompt="故宫在哪里？",
            model_output="故宫位于北京。",
            reference_answer="北京",
            retrieved_evidence=[],
            lang="zh",
            source="faq_zh",
        ),
        _record(
            "zh-004",
            prompt="标准大气压下水的沸点是多少？",
            model_output="水的沸点是100摄氏度。",
            reference_answer="100摄氏度",
            retrieved_evidence=["标准大气压下，水的沸点是100摄氏度。"],
            lang="zh",
            source="faq_zh",
            annotation={"is_hallucination": False, "needs_review": False},
        ),
        _record(
            "en-001",
            prompt="Where is the Eiffel Tower?",
            model_output="The Eiffel Tower is located in Paris.",
            reference_answer="Paris",
            retrieved_evidence=["The Eiffel Tower is located in Paris."],
            lang="en",
            source="news_en",
            annotation={"is_hallucination": False, "needs_review": False},
        ),
        _record(
            "en-002",
            prompt="When was the Eiffel Tower completed?",
            model_output="The Eiffel Tower was completed in 1900.",
            reference_answer="1889",
            retrieved_evidence=["The Eiffel Tower was completed in 1889."],
            lang="en",
            source="news_en",
            annotation={"is_hallucination": True, "needs_review": False},
        ),
        _record(
            "mix-001",
            prompt="巴黎和卢浮宫在哪里？",
            model_output="巴黎是法国的首都，the louvre is located in paris.",
            reference_answer="法国巴黎",
            retrieved_evidence=["巴黎是法国的首都。", "The Louvre is located in Paris."],
            lang="mixed",
            source="wiki_mixed",
            annotation={"is_hallucination": False, "needs_review": False},
        ),
        _record(
            "mix-002",
            prompt="巴黎和卢浮宫在哪里？",
            model_output="巴黎是法国的首都，the louvre is located in berlin.",
            reference_answer="法国巴黎",
            retrieved_evidence=["巴黎是法国的首都。", "The Louvre is located in Paris."],
            lang="mixed",
            source="wiki_mixed",
            annotation={"is_hallucination": True, "needs_review": False},
        ),
        _record(
            "en-003",
            prompt="Which planet is fourth from the Sun?",
            model_output="Mars is the fourth planet from the sun.",
            reference_answer="Mars",
            retrieved_evidence=["Mars is the fourth planet from the Sun."],
            lang="en",
            source="news_en",
        ),
        _record(
            "en-004",
            prompt="What are the moons of Mars called?",
            model_output="Mars has two moons named luna and titan.",
            reference_answer="Phobos and Deimos",
            retrieved_evidence=["Mars has two moons named Phobos and Deimos."],
            lang="en",
            source="news_en",
        ),
        _record(
            "zh-005",
            prompt="水的沸点是多少？",
            model_output="",
            reference_answer="100摄氏度",
            retrieved_evidence=["水的沸点是100摄氏度。"],
            lang="zh",
            source="faq_zh",
        ),
        _record(
            "mix-003",
            prompt="What is the Sun?",
            model_output="ＴＨＥ   ＳＵＮ is a star。",
            reference_answer="a star",
            retrieved_evidence=["The Sun is a star."],
            lang="mixed",
            source="wiki_mixed",
            annotation={"is_hallucination": False, "needs_review": False},
        ),
        _record(
            "en-005",
            prompt="What is the Moon made of?",
            model_output="The moon is made of cheese.",
            reference_answer="rock and metal",
            retrieved_evidence=["The Moon is a natural satellite of Earth."],
            lang="en",
            source="web_en",
            annotation={"is_hallucination": True, "needs_review": False},
        ),
        _record(
            "en-006",
            prompt="What is the Moon?",
            model_output="The moon is a natural satellite of earth.",
            reference_answer="a natural satellite",
            retrieved_evidence=["The Moon is a natural satellite of Earth."],
            lang="en",
            source="web_en",
        ),
        _record(
            "zh-006",
            prompt="法国的首都是哪里？",
            model_output="法国首都是巴黎。",
            reference_answer="巴黎",
            retrieved_evidence=["巴黎是法国的首都。"],
            lang="zh",
            source="kb_zh",
            annotation={"is_hallucination": False, "needs_review": False},
        ),
        _record(
            "mix-004",
            prompt="水的沸点是多少？",
            model_output="标准大气压下，水的沸点是100摄氏度。",
            reference_answer="100摄氏度",
            retrieved_evidence=["标准大气压下，水的沸点是100摄氏度。"],
            lang="mixed",
            source="faq_zh",
            annotation={"is_hallucination": False, "needs_review": False},
        ),
        _record(
            "en-007",
            prompt="When was the Eiffel Tower completed?",
            model_output="The eiffel tower was completed in 1889.",
            reference_answer="1889",
            retrieved_evidence=["The Eiffel Tower was completed in 1889."],
            lang="en",
            source="news_en",
            annotation={"is_hallucination": False, "needs_review": False},
        ),
        _record(
            "zh-007",
            prompt="巴黎是法国的首都吗？",
            model_output="巴黎不是法国的首都。",
            reference_answer="是",
            retrieved_evidence=["巴黎是法国的首都。"],
            lang="zh",
            source="kb_zh",
            annotation={"is_hallucination": True, "needs_review": False},
        ),
        _record(
            "mix-005",
            prompt="Where is the Louvre?",
            model_output="the louvre is located in paris.",
            reference_answer="Paris",
            retrieved_evidence=["The Louvre is located in Paris."],
            lang="mixed",
            source="wiki_mixed",
            annotation={"is_hallucination": True, "needs_review": True},
        ),
        _record(
            "zh-008",
            prompt="故宫在哪里？",
            model_output="故宫位于北京。",
            reference_answer="北京",
            retrieved_evidence=[""],
            lang="zh",
            source="faq_zh",
        ),
    ]
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description="生成确定性的本地 JSONL 评测分片")
    parser.add_argument("--out-dir", default="data/demo")
    parser.add_argument("--seed", type=int, default=20261123)
    parser.add_argument("--shards", type=int, default=3)
    args = parser.parse_args()
    if args.shards < 3:
        raise SystemExit("--shards 必须至少为 3")

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("shard-*.jsonl"):
        old.unlink()

    records = build_records()
    random.Random(args.seed).shuffle(records)
    shards: list[list[dict[str, Any]]] = [[] for _ in range(args.shards)]
    for index, record in enumerate(records):
        shards[index % args.shards].append(record)

    shard_names: list[str] = []
    for index, shard_records in enumerate(shards):
        name = f"shard-{index:03d}.jsonl"
        shard_names.append(name)
        with jsonlines.open(
            out_dir / name,
            mode="w",
            compact=True,
            sort_keys=True,
            flush=True,
        ) as writer:
            for record in shard_records:
                writer.write(record)

    observed = sorted({(record["source"], record["lang"]) for record in records})
    declared = [{"source": source, "lang": lang} for source, lang in observed]
    declared.extend(
        [
            {"source": "kb_zh", "lang": "ja"},
            {"source": "shadow_source", "lang": "en"},
        ]
    )
    manifest = {
        "schema_version": "1.0",
        "seed": args.seed,
        "record_count": len(records),
        "shard_count": len(shard_names),
        "shards": shard_names,
        "declared_strata": declared,
        "coverage_notes": {
            "mixed_language": True,
            "empty_evidence": True,
            "empty_output": True,
            "missing_annotation": True,
            "declared_empty_strata": 2,
        },
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    invalid_cases = {
        "schema_version": "1.0",
        "cases": [
            {
                "name": "invalid_sentence_support_threshold",
                "edits": ["sentence_support_threshold=1.5"],
                "expected_code": "E_INVALID_CONFIG",
            },
            {
                "name": "zero_latency_budget",
                "query": "sample",
                "query_id": "demo-budget-0",
                "budget_ms": 0,
                "expected_code": "E_LATENCY_BUDGET",
            },
        ],
    }
    (out_dir / "invalid_cases.json").write_text(
        json.dumps(invalid_cases, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "out_dir": str(out_dir),
                "seed": args.seed,
                "records": len(records),
                "shards": shard_names,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
