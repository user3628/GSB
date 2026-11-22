from __future__ import annotations

import json
import random
from collections import Counter
from pathlib import Path
from typing import Any

from . import yaml_lite
from .data import load_dataset
from .util import atomic_write_json, atomic_write_jsonl, atomic_write_text, sha256_file


LABELS = ("account", "billing", "content", "fraud", "privacy", "security", "spam")
SMALL_LABELS = ("account", "billing", "content", "fraud")
LARGE_LABELS = LABELS
SMALL_WEIGHTS = (0.70, 0.20, 0.08, 0.02)
LARGE_WEIGHTS = (0.48, 0.22, 0.12, 0.08, 0.05, 0.03, 0.02)
LANGUAGES = ("zh", "en", "mixed")
LANGUAGE_WEIGHTS = (0.45, 0.35, 0.20)
CHANNELS = ("api", "app", "import", "web")
CHANNEL_WEIGHTS = (0.18, 0.32, 0.14, 0.36)


TERMS: dict[str, dict[str, list[str]]] = {
    "account": {
        "zh": ["账户", "登录", "密码", "用户", "验证码", "个人资料", "认证", "锁定"],
        "en": ["account", "login", "password", "profile", "verification", "credential", "locked", "signin"],
    },
    "billing": {
        "zh": ["账单", "支付", "退款", "扣费", "订单", "发票", "金额", "订阅"],
        "en": ["billing", "payment", "refund", "invoice", "order", "charge", "amount", "subscription"],
    },
    "content": {
        "zh": ["内容", "文章", "评论", "审核", "文本", "标题", "段落", "发布"],
        "en": ["content", "article", "comment", "moderation", "text", "title", "paragraph", "publish"],
    },
    "fraud": {
        "zh": ["欺诈", "诈骗", "盗刷", "异常", "盗用", "冒充", "资金", "报警"],
        "en": ["fraud", "scam", "chargeback", "stolen", "impersonation", "suspicious", "fund", "alert"],
    },
    "privacy": {
        "zh": ["隐私", "个人信息", "授权", "删除", "同意", "数据", "保留", "肖像"],
        "en": ["privacy", "personal", "consent", "delete", "retention", "data", "portrait", "request"],
    },
    "security": {
        "zh": ["攻击", "漏洞", "安全", "入侵", "恶意软件", "告警", "补丁", "泄露"],
        "en": ["attack", "vulnerability", "security", "breach", "malware", "incident", "patch", "leak"],
    },
    "spam": {
        "zh": ["广告", "垃圾", "推广", "营销", "群发", "链接", "优惠", "刷屏"],
        "en": ["spam", "promotion", "bulk", "marketing", "link", "discount", "blast", "unsolicited"],
    },
}
COMMON = {
    "zh": ["请", "处理", "今天", "系统", "记录", "用户", "反馈", "需要"],
    "en": ["please", "handle", "today", "system", "record", "customer", "report", "review"],
}
NOISE = ["urgent", "普通", "sample", "批次", "update", "case", "备注", "queue"]


def _weighted_risk(label: str, rng: random.Random) -> int:
    if label in {"fraud", "security"}:
        return rng.choices((1, 2, 3), weights=(0.2, 0.45, 0.35), k=1)[0]
    if label in {"privacy", "spam"}:
        return rng.choices((0, 1, 2, 3), weights=(0.2, 0.4, 0.3, 0.1), k=1)[0]
    return rng.choices((0, 1, 2, 3), weights=(0.45, 0.35, 0.15, 0.05), k=1)[0]


def _sentence(terms: list[str], rng: random.Random, language: str) -> str:
    start = rng.randrange(len(terms))
    selected = [terms[(start + offset) % len(terms)] for offset in range(5)]
    if language == "zh":
        return "，".join(selected[:4]) + "。" + selected[4]
    if language == "en":
        return " ".join(selected[:5]).capitalize() + "."
    return "，".join(selected[:3]) + "。 " + " ".join(word.upper() if index == 0 else word for index, word in enumerate(selected[3:]))


def make_text(label: str, language: str, rng: random.Random) -> str:
    terms = TERMS[label]
    pieces: list[str] = []
    if language in {"zh", "mixed"}:
        pieces.append(_sentence(terms["zh"], rng, "zh"))
        common = rng.sample(COMMON["zh"], 2)
        pieces.append(" ".join(common) + "  " + rng.choice(NOISE))
    if language in {"en", "mixed"}:
        pieces.append(_sentence(terms["en"], rng, "en").upper() if rng.random() < 0.25 else _sentence(terms["en"], rng, "en"))
        pieces.append(" ".join(rng.sample(COMMON["en"], 2)) + "  " + rng.choice(NOISE))
    body = " \n ".join(pieces)
    if rng.random() < 0.08:
        body = body.replace("account", "ＡＣＣＯＵＮＴ").replace("billing", "ＢＩＬＬＩＮＧ")
    if rng.random() < 0.04:
        body = body.replace("0", "０").replace("1", "１")
    return "  " + body + "  "


def _batch_specs() -> list[dict[str, Any]]:
    return [
        {"batch_id": "batch_01_small", "batch_no": 1, "tier": "small", "rows": 400, "labels": SMALL_LABELS, "weights": SMALL_WEIGHTS},
        {"batch_id": "batch_02_small", "batch_no": 2, "tier": "small", "rows": 400, "labels": SMALL_LABELS, "weights": SMALL_WEIGHTS},
        {"batch_id": "batch_03_large", "batch_no": 3, "tier": "large", "rows": 4000, "labels": LARGE_LABELS, "weights": LARGE_WEIGHTS},
        {"batch_id": "batch_04_large", "batch_no": 4, "tier": "large", "rows": 4000, "labels": LARGE_LABELS, "weights": LARGE_WEIGHTS},
    ]


def _config_document() -> dict[str, Any]:
    return {
        "version": "demo-2026-11-23-v1",
        "hash_dim": 1024,
        "smoothing": 0.35,
        "unknown_token": "<UNK>",
        "labels": list(LABELS),
        "thresholds": {
            "account": 0.10,
            "billing": 0.10,
            "content": 0.10,
            "fraud": 0.05,
            "privacy": 0.05,
            "security": 0.05,
            "spam": 0.05,
        },
        "target_fpr": 0.08,
        "threshold_max": 0.98,
        "evaluation_budget": 9000,
        "workers": 4,
        "class_weights": {
            "account": 1.0,
            "billing": 1.2,
            "content": 1.5,
            "fraud": 2.0,
            "privacy": 2.5,
            "security": 3.0,
            "spam": 4.0,
        },
        "calibration": {
            "fold_modulus": 5,
            "fold_remainder": 0,
            "min_samples_per_class": 5,
        },
    }


def generate_demo(output_dir: Path | str, seed: int = 20261123) -> dict[str, Any]:
    root = Path(output_dir).resolve()
    batches_dir = root / "batches"
    batches_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    index_batches: list[dict[str, Any]] = []
    for spec in _batch_specs():
        rows: list[dict[str, Any]] = []
        labels = tuple(spec["labels"])
        for offset in range(spec["rows"]):
            doc_number = offset + 1
            label = rng.choices(labels, weights=spec["weights"], k=1)[0]
            language = rng.choices(LANGUAGES, weights=LANGUAGE_WEIGHTS, k=1)[0]
            channel = rng.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]
            if doc_number % 397 == 0:
                text = "   \n\t  "
            elif language == "mixed" and doc_number % 211 == 0:
                text = "中英 mixed 文本\n with  extra   spaces and ＡＣＣＯＵＮＴ"
            else:
                text = make_text(label, language, rng)
            doc_id = f"b{spec['batch_no']:02d}-{doc_number:06d}"
            rows.append(
                {
                    "doc_id": doc_id,
                    "text": text,
                    "lang": language,
                    "channel": channel,
                    "label": label,
                    "risk_level": _weighted_risk(label, rng),
                }
            )
        rows.sort(key=lambda row: row["doc_id"])
        relative = f"batches/{spec['batch_id']}.jsonl"
        path = root / relative
        atomic_write_jsonl(path, rows)
        index_batches.append(
            {
                "batch_id": spec["batch_id"],
                "batch_no": spec["batch_no"],
                "tier": spec["tier"],
                "path": relative,
                "row_count": len(rows),
                "labels": sorted({row["label"] for row in rows}),
                "languages": sorted({row["lang"] for row in rows}),
                "channels": sorted({row["channel"] for row in rows}),
                "content_digest": sha256_file(path),
            }
        )
    index = {"schema": "demo-dataset-v1", "seed": seed, "batches": index_batches}
    atomic_write_json(root / "index.json", index)
    config = _config_document()
    atomic_write_text(root / "config.yaml", yaml_lite.dumps(config))
    return {
        "seed": seed,
        "root": str(root),
        "config": str(root / "config.yaml"),
        "index": str(root / "index.json"),
        "batch_count": len(index_batches),
        "row_count": sum(batch["row_count"] for batch in index_batches),
    }


def verify_demo(index_path: Path | str) -> dict[str, Any]:
    dataset = load_dataset(index_path)
    batches: list[dict[str, Any]] = []
    tier_rows: Counter[str] = Counter()
    total_empty = 0
    all_doc_ids: set[str] = set()
    for batch in dataset.batches:
        rows = []
        with open(batch.path, "r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    rows.append(json.loads(line))
        labels = Counter(row["label"] for row in rows)
        languages = Counter(row["lang"] for row in rows)
        channels = Counter(row["channel"] for row in rows)
        risks = Counter(row.get("risk_level") for row in rows)
        empty = sum(1 for row in rows if not row.get("text", "").strip())
        total_empty += empty
        for row in rows:
            if row["doc_id"] in all_doc_ids:
                raise ValueError(f"duplicate doc_id: {row['doc_id']}")
            all_doc_ids.add(row["doc_id"])
        tier_rows[batch.tier] += len(rows)
        batches.append(
            {
                "batch_id": batch.batch_id,
                "batch_no": batch.batch_no,
                "tier": batch.tier,
                "row_count": len(rows),
                "label_counts": {key: labels[key] for key in sorted(labels)},
                "language_counts": {key: languages[key] for key in sorted(languages)},
                "channel_counts": {key: channels[key] for key in sorted(channels)},
                "risk_level_counts": {str(key): risks[key] for key in sorted(risks, key=lambda value: (value is None, value))},
                "empty_doc_count": empty,
                "content_digest_matches_index": batch.content_digest == sha256_file(batch.path),
            }
        )
    label_ratios = [
        max(item["label_counts"].values()) / min(item["label_counts"].values())
        for item in batches
        if item["label_counts"]
    ]
    checks = {
        "batch_numbers_contiguous": [item["batch_no"] for item in batches]
        == list(range(1, len(batches) + 1)),
        "at_least_four_batches": len(batches) >= 4,
        "small_total_near_800": 700 <= tier_rows["small"] <= 900,
        "large_total_near_8000": 7500 <= tier_rows["large"] <= 8500,
        "small_batches_have_four_labels": all(
            len(item["label_counts"]) == 4 for item in batches if item["tier"] == "small"
        ),
        "large_batches_have_seven_labels": all(
            len(item["label_counts"]) == 7 for item in batches if item["tier"] == "large"
        ),
        "contains_empty_docs": total_empty > 0,
        "valid_language_tags": all(
            set(item["language_counts"]) <= {"zh", "en", "mixed"} for item in batches
        ),
        "valid_channels": all(
            set(item["channel_counts"]) <= {"api", "app", "import", "web"}
            for item in batches
        ),
        "valid_risk_levels": all(
            set(item["risk_level_counts"]) <= {"0", "1", "2", "3"} for item in batches
        ),
        "class_distribution_imbalanced": bool(label_ratios) and min(label_ratios) >= 5.0,
        "unique_doc_ids": len(all_doc_ids) == sum(item["row_count"] for item in batches),
        "all_batch_hashes_match": all(item["content_digest_matches_index"] for item in batches),
    }
    return {
        "schema": "demo-data-verification-v1",
        "seed": dataset.seed,
        "batch_count": len(batches),
        "row_count": sum(item["row_count"] for item in batches),
        "tier_counts": {key: tier_rows[key] for key in sorted(tier_rows)},
        "empty_doc_count": total_empty,
        "minimum_batch_max_to_min_class_ratio": min(label_ratios) if label_ratios else None,
        "checks": checks,
        "passed": all(checks.values()),
        "batches": batches,
    }
