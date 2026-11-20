#!/usr/bin/env python3
"""Generate the fixed local Chinese/English JSONL corpus and YAML config."""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any

import yaml


SEED = 20261120


def canonical_json_line(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def build_documents() -> list[dict[str, Any]]:
    rng = random.Random(SEED)
    documents: list[dict[str, Any]] = []

    topics = [
        (
            "中文检索与倒排索引",
            "倒排索引把文档映射到词项 posting。中文检索需要处理全角字符、空白与停用词。",
            "zh",
        ),
        (
            "Compressed Search Pipeline",
            "A sparse inverted index compresses doc ids with delta encoding and LEB128 varints.",
            "en",
        ),
        (
            "中英混排 Search 压缩",
            "中英混合文档包含 Index、BM25、跳表与 cache。压缩查询必须保持命中集合一致。",
            "mixed",
        ),
        (
            "向量之外：Sparse Retrieval",
            "Sparse retrieval 使用词项权重；BM25 参数变化会影响得分与缓存指纹。",
            "mixed",
        ),
        (
            "本地确定性验证",
            "固定随机种子保证 JSONL 分片可重建，摘要与查询命中清单可复现。",
            "zh",
        ),
        (
            "Checkpoint Recovery",
            "Graceful stop happens only at a document or term boundary. A checkpoint is atomic.",
            "en",
        ),
        (
            "跳表与分块解压",
            "倒排列表按块存储，跳表记录步长和 doc id；查询只按需解压命中的分块。",
            "zh",
        ),
        (
            "Cache Correctness",
            "LRU eviction must never alter hits or scores. Cache keys include config and artifact digests.",
            "en",
        ),
    ]
    running = 1
    for shard in range(1, 4):
        for local_index in range(1, 11):
            topic_title, topic_body, language = topics[(running + shard) % len(topics)]
            suffix_zh = ["检索", "索引", "压缩", "查询", "分块", "权重", "文档", "缓存"][local_index % 8]
            suffix_en = ["search", "index", "posting", "cache", "varint", "skip"][running % 6]
            body = f"{topic_body} 主题词：{suffix_zh}。 English marker: {suffix_en}-{local_index}."
            title = f"{topic_title} #{running:04d}"
            doc_id = f"doc-{shard:02d}-{local_index:04d}"
            documents.append(
                {
                    "doc_id": doc_id,
                    "title": title,
                    "body": body,
                    "lang": language,
                    "source": f"shard-{shard:03d}",
                }
            )
            running += 1

    # Cross-shard near duplicates with distinct doc ids preserve the duplicate
    # retrieval case while keeping postings well-defined.
    documents.append(
        {
            "doc_id": "doc-04-0001",
            "title": documents[0]["title"],
            "body": documents[0]["body"],
            "lang": documents[0]["lang"],
            "source": "shard-004",
        }
    )
    documents.append(
        {
            "doc_id": "doc-04-0002",
            "title": documents[12]["title"],
            "body": documents[12]["body"].replace("posting", "postings"),
            "lang": documents[12]["lang"],
            "source": "shard-004",
        }
    )
    # Empty bodies are retained and explicitly marked by stage 1.  These docs
    # can still match through their titles.
    documents.append(
        {
            "doc_id": "doc-04-0003",
            "title": "空正文档 Empty Body 检索",
            "body": "",
            "lang": "mixed",
            "source": "shard-004",
        }
    )
    documents.append(
        {
            "doc_id": "doc-04-0004",
            "title": "全空文档",
            "body": "   \t  ",
            "lang": "zh",
            "source": "shard-004",
        }
    )
    return documents


def write_shards(data_dir: Path, documents: list[dict[str, Any]]) -> None:
    shard_dir = data_dir / "shards"
    for old in shard_dir.glob("*.jsonl"):
        old.unlink()
    shard_dir.mkdir(parents=True, exist_ok=True)
    shards: dict[str, list[dict[str, Any]]] = {}
    for document in documents:
        shards.setdefault(document["source"], []).append(document)
    for source in sorted(shards):
        destination = shard_dir / f"{source}.jsonl"
        lines = [canonical_json_line(document) for document in shards[source]]
        destination.write_text("\n".join(lines) + "\n", encoding="utf-8")


def base_config() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "index_version": "local-cn-sparse-v1",
        "documents": {"glob": "shards/*.jsonl"},
        "tokenizer": {
            "unicode_normalization": "NFKC",
            "whitespace_folding": True,
            "lowercase": True,
            "han_unigrams": True,
            "han_adjacent_bigrams": True,
            "latin_alnum_runs": True,
        },
        "stopwords": ["的", "了", "与", "和", "a", "an", "the", "of", "to", "is"],
        "bm25": {"k1": 1.2, "b": 0.75},
        "quantization": {"mode": "fixed_point", "scale": 10000},
        "postings": {
            "block_size": 6,
            "skip_step": 4,
            "delta_encoding": True,
            "varint_encoding": "unsigned_leb128",
        },
        "query": {
            "cache_capacity": 3,
            "latency_budget_ms": 1000.0,
            "max_limit": 1000,
            "default_limit": 20,
        },
        "queries": "queries.jsonl",
    }


def write_configs(data_dir: Path) -> None:
    config = base_config()
    (data_dir / "index.yaml").write_text(
        yaml.safe_dump(config, allow_unicode=True, sort_keys=True),
        encoding="utf-8",
    )
    invalid = json.loads(json.dumps(config))
    invalid["postings"]["skip_step"] = 0
    invalid["documents"]["glob"] = "../shards/*.jsonl"
    invalid["queries"] = "../queries.jsonl"
    error_dir = data_dir / "error_cases"
    error_dir.mkdir(parents=True, exist_ok=True)
    (error_dir / "invalid_skip_step.yaml").write_text(
        yaml.safe_dump(invalid, allow_unicode=True, sort_keys=True),
        encoding="utf-8",
    )
    out_of_bounds = {
        "query_id": "q-oob",
        "terms": ["检索", "索引"],
        "mode": "and",
        "limit": 0,
    }
    (error_dir / "query_out_of_bounds.json").write_text(
        json.dumps(out_of_bounds, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    zero_budget = json.loads(json.dumps(config))
    zero_budget["query"]["latency_budget_ms"] = 0
    zero_budget["documents"]["glob"] = "../shards/*.jsonl"
    zero_budget["queries"] = "../queries.jsonl"
    (error_dir / "latency_budget_zero.yaml").write_text(
        yaml.safe_dump(zero_budget, allow_unicode=True, sort_keys=True),
        encoding="utf-8",
    )


def write_queries(data_dir: Path) -> None:
    queries = [
        {"query_id": "q-and-01", "terms": ["检索", "索引"], "mode": "and", "limit": 20},
        {"query_id": "q-and-02", "terms": ["中文", "压缩"], "mode": "and", "limit": 20},
        {"query_id": "q-and-03", "terms": ["search", "cache"], "mode": "and", "limit": 20},
        {"query_id": "q-and-04", "terms": ["空正", "检索"], "mode": "and", "limit": 20},
        {"query_id": "q-or-01", "terms": ["跳表", "varint"], "mode": "or", "limit": 20},
        {"query_id": "q-or-02", "terms": ["文档", "posting", "checkpoint"], "mode": "or", "limit": 20},
        {"query_id": "q-or-03", "terms": ["全空", "索引", "query"], "mode": "or", "limit": 20},
        {"query_id": "q-and-unknown", "terms": ["检索", "不存在的词项"], "mode": "and", "limit": 20},
    ]
    lines = [canonical_json_line(query) for query in queries]
    (data_dir / "queries.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data")
    args = parser.parse_args()
    data_dir = Path(args.data_dir).resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    documents = build_documents()
    write_shards(data_dir, documents)
    write_configs(data_dir)
    write_queries(data_dir)
    print(
        json.dumps(
            {
                "seed": SEED,
                "document_count": len(documents),
                "shard_count": len(list((data_dir / "shards").glob("*.jsonl"))),
                "empty_body_count": sum(1 for doc in documents if not doc["body"].strip()),
                "data_dir": str(data_dir),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
