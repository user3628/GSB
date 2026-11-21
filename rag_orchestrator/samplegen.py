"""Deterministic local JSONL/YAML sample generator."""

from __future__ import annotations

import random
from pathlib import Path
from typing import Any

from .config import default_config, dump_config, validate_config
from .util import atomic_write_bytes, atomic_write_json, sha256_bytes, write_jsonl


DEFAULT_SEED = 20261122


def _documents() -> list[dict[str, Any]]:
    return [
        {
            "doc_id": "doc-001",
            "title": "并行检索与预算裁决",
            "body": (
                "RAG 四阶段编排以子查询为调度单元。全局检索配额、单并发通道配额与请求延迟预算同时生效；"
                "超预算子查询显式降级为未处理，不阻塞等待。线程池按通道并行执行，确定性归约保证并行与串行逐字节一致。"
            ),
            "lang": "zh-CN",
            "source": "official",
            "shard": "shard-01",
        },
        {
            "doc_id": "doc-002",
            "title": "检查点恢复与中断一致性",
            "body": (
                "checkpoint records use atomic replace and fsync. A stop request finishes the current indivisible stage, "
                "then exits. Resume reuses fingerprint-matched stages, keeps sequence numbers contiguous, and preserves "
                "byte-for-byte answer, citation, and budget details. 检查点 序号 连续推进。"
            ),
            "lang": "en-US",
            "source": "official",
            "shard": "shard-01",
        },
        {
            "doc_id": "doc-003",
            "title": "Parallel budget checkpoint",
            "body": (
                "parallel scheduling combines global quota, channel quota, and latency budget. checkpoint resume is "
                "deterministic; citation attribution remains stable after recovery. 并行 预算 检查点 恢复。"
            ),
            "lang": "en-US",
            "source": "official",
            "shard": "shard-02",
        },
        {
            "doc_id": "doc-003",
            "title": "Parallel budget checkpoint",
            "body": (
                "parallel scheduling combines global quota, channel quota, and latency budget. checkpoint resume is "
                "deterministic; citation attribution remains stable after recovery. 并行 预算 检查点 恢复。"
            ),
            "lang": "en-US",
            "source": "official",
            "shard": "shard-03",
        },
        {
            "doc_id": "doc-004",
            "title": "退款政策 Refund Policy",
            "body": (
                "退款申请在审核通过后 7 个工作日内原路退回。refund timeline is seven business days after approval; "
                "跨境支付可能增加 1 到 2 个工作日。"
            ),
            "lang": "zh-CN",
            "source": "official",
            "shard": "shard-02",
        },
        {
            "doc_id": "doc-005",
            "title": "版本指纹与增量失效",
            "body": (
                "各阶段内容摘要构成版本指纹。文档、同义词、分解规则、重排权重、预算配置或证据条数变化时，"
                "从最早受影响阶段开始失效并重建下游；未受影响的上游 checkpoint 继续复用。fingerprint invalidation。"
            ),
            "lang": "zh-CN",
            "source": "faq",
            "shard": "shard-02",
        },
        {
            "doc_id": "doc-006",
            "title": "引用归属与证据拼接 Citation Coverage",
            "body": (
                "Evidence is reranked by retrieval score, query coverage, and fragment length fit. MMR overlap removal "
                "records discarded fragments. Every retained evidence fragment must have a citation doc_id; the local "
                "answer template prints citation attribution for all evidence. 引用 证据 拼接。"
            ),
            "lang": "en-US",
            "source": "official",
            "shard": "shard-03",
        },
        {
            "doc_id": "doc-007",
            "title": "Latency budgets and explicit degradation",
            "body": (
                "Latency budgets are measured from actual stage completion time, never by sleeping or declaring a "
                "thread timeout. Budget overruns return E_LATENCY_BUDGET with elapsed time and accepted subquery count. "
                "超预算 显式 降级。"
            ),
            "lang": "en-US",
            "source": "community",
            "shard": "shard-03",
        },
        {
            "doc_id": "doc-008",
            "title": "查询规范化与同义词",
            "body": (
                "查询先做 Unicode NFKC、空白折叠与全半角统一，再按中文单字和拉丁连续串切分。分解规则与本地同义词表"
                "生成带优先级的子查询。query normalization uses deterministic tokenization."
            ),
            "lang": "zh-CN",
            "source": "faq",
            "shard": "shard-03",
        },
    ]


def _requests() -> list[dict[str, Any]]:
    long_query = "；".join(
        [
            "并行检索预算",
            "检查点恢复",
            "版本指纹失效",
            "证据重排拼接",
            "引用归属覆盖",
            "延迟预算判定",
            "全局配额",
            "通道配额",
            "线程池归约",
            "同义词扩展",
            "中文英文 mixed",
            "异常请求验证",
        ]
    ) + "？"
    return [
        {
            "query_id": "q-001",
            "query": "RAG 并行检索如何执行预算裁决？",
            "locale": "zh-CN",
            "channel": "api",
            "expected_doc_id": "doc-001",
        },
        {
            "query_id": "q-002",
            "query": "checkpoint 恢复后如何保证 byte-for-byte 一致？",
            "locale": "en-US",
            "channel": "web",
            "expected_doc_id": "doc-002",
        },
        {
            "query_id": "q-003",
            "query": "退款 refund 时限多久？",
            "locale": "zh-CN",
            "channel": "kiosk",
            "expected_doc_id": "doc-004",
        },
        {
            "query_id": "q-004",
            "query": "What is the citation coverage rule?",
            "locale": "en-US",
            "channel": "web",
            "expected_doc_id": "doc-006",
        },
        {
            "query_id": "q-005",
            "query": "版本指纹如何触发增量失效？",
            "locale": "zh-CN",
            "channel": "api",
            "expected_doc_id": "doc-005",
        },
        {
            "query_id": "q-006",
            "query": "　  ",
            "locale": "zh-CN",
            "channel": "api",
            "expected_doc_id": None,
        },
        {
            "query_id": "q-007",
            "query": long_query,
            "locale": "zh-CN",
            "channel": "api",
            "expected_doc_id": None,
        },
    ]


def generate_samples(out_dir: str | Path, seed: int = DEFAULT_SEED) -> dict[str, Any]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    shard_dir = out / "shards"
    shard_dir.mkdir(parents=True, exist_ok=True)

    rng = random.Random(seed)
    documents = _documents()
    shards: dict[str, list[dict[str, Any]]] = {}
    for doc in documents:
        shards.setdefault(doc["shard"], []).append(doc)
    for shard_name in sorted(shards):
        rng.shuffle(shards[shard_name])
        shards[shard_name].sort(key=lambda item: (item["doc_id"], item["title"]))
        write_jsonl(shard_dir / f"{shard_name}.jsonl", shards[shard_name])

    merged_documents = sorted(
        documents,
        key=lambda item: (item["doc_id"], item["shard"], item["title"]),
    )
    write_jsonl(out / "docs.jsonl", merged_documents)

    requests = _requests()
    channels = ["api", "web", "kiosk", "batch"]
    for request in requests:
        if request["query_id"].startswith("q-") and request["query_id"] not in {"q-006", "q-007"}:
            request["channel"] = rng.choice(channels)
    requests.sort(key=lambda item: item["query_id"])
    write_jsonl(out / "requests.jsonl", requests)

    config = validate_config(default_config())
    config_text = dump_config(config)
    atomic_write_bytes(out / "config.yaml", config_text.encode("utf-8"), mode=0o644)

    manifest = {
        "seed": seed,
        "generator_version": "samplegen-v1",
        "document_records": len(documents),
        "unique_doc_ids": len({doc["doc_id"] for doc in documents}),
        "shards": sorted(shards),
        "request_count": len(requests),
        "files": {},
    }
    for relative in ["docs.jsonl", "requests.jsonl", "config.yaml"]:
        data = (out / relative).read_bytes()
        manifest["files"][relative] = {"sha256": sha256_bytes(data), "bytes": len(data)}
    for shard_name in sorted(shards):
        relative = f"shards/{shard_name}.jsonl"
        data = (out / relative).read_bytes()
        manifest["files"][relative] = {"sha256": sha256_bytes(data), "bytes": len(data)}
    atomic_write_json(out / "manifest.json", manifest, mode=0o644)
    return manifest
