from __future__ import annotations

import math
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .util import atomic_write_bytes, canonical_json_bytes


FEATURE_DIM = 8


@dataclass(frozen=True)
class ScaleSpec:
    name: str
    item_count: int
    query_count: int
    seed_offset: int


SCALES: tuple[ScaleSpec, ...] = (
    ScaleSpec("small", 200, 12, 0),
    ScaleSpec("medium", 2000, 24, 1),
    ScaleSpec("large", 12000, 48, 2),
)


CATEGORY_VOCAB: dict[str, tuple[str, ...]] = {
    "electronics": ("wireless", "headphones", "speaker", "camera", "charger", "无线", "耳机", "相机", "充电器"),
    "books": ("novel", "history", "poetry", "guide", "science", "小说", "历史", "诗歌", "科学"),
    "sports": ("running", "shoes", "yoga", "cycling", "outdoor", "跑步", "球鞋", "瑜伽", "骑行"),
    "home": ("coffee", "kitchen", "lamp", "storage", "cleaner", "咖啡", "厨房", "台灯", "收纳"),
    "beauty": ("skincare", "lipstick", "fragrance", "serum", "mask", "护肤", "口红", "香水", "面膜"),
    "toys": ("puzzle", "robot", "building", "plush", "game", "拼图", "机器人", "积木", "玩偶"),
}


CATEGORIES = tuple(CATEGORY_VOCAB)
GENERIC_TERMS = ("new", "classic", "portable", "家庭", "便携", "经典", "精选")


def _tokenize(text: str) -> set[str]:
    import re

    return {token.casefold() for token in re.findall(r"[A-Za-z0-9]+|[\u3400-\u9fff]", text)}


def _category_centroid(category: str) -> tuple[float, ...]:
    index = CATEGORIES.index(category) + 1
    values = [math.sin(index * (dimension + 1) * 0.73) for dimension in range(FEATURE_DIM)]
    norm = math.sqrt(sum(value * value for value in values)) or 1.0
    return tuple(round(value / norm, 8) for value in values)


def _round_features(values: Iterable[float]) -> list[float]:
    return [round(float(value), 8) for value in values]


def _write_jsonl(path: Path, rows: Iterable[dict]) -> None:
    lines = [canonical_json_bytes(row) for row in rows]
    data = b"\n".join(lines) + (b"\n" if lines else b"")
    atomic_write_bytes(path, data)


def _make_item(rng: random.Random, scale: str, index: int, category: str) -> dict:
    vocab = CATEGORY_VOCAB[category]
    title_terms: list[str] = []
    for _ in range(rng.randint(2, 4)):
        title_terms.append(rng.choice(vocab))
    if rng.random() < 0.5:
        title_terms.append(rng.choice(GENERIC_TERMS))
    centroid = _category_centroid(category)
    features = [centroid[dimension] + rng.gauss(0.0, 0.30) for dimension in range(FEATURE_DIM)]
    return {
        "item_id": f"{scale}-{index:06d}",
        "category": category,
        "title": " ".join(title_terms),
        "features": _round_features(features),
    }


def _relevance(item: dict, query: dict) -> float:
    query_terms = _tokenize(query["query_text"])
    item_terms = _tokenize(item["title"])
    overlap = len(query_terms & item_terms)
    same_category = 1.0 if item["category"] in query["history_categories"] else 0.0
    item_features = item["features"]
    query_features = query["query_features"]
    dot = math.fsum(a * b for a, b in zip(item_features, query_features))
    item_norm = math.sqrt(math.fsum(value * value for value in item_features)) or 1.0
    query_norm = math.sqrt(math.fsum(value * value for value in query_features)) or 1.0
    cosine = dot / (item_norm * query_norm)
    popularity = math.fsum(abs(value) for value in item_features) / FEATURE_DIM
    return 2.5 * overlap + 1.5 * same_category + 0.8 * cosine + 0.05 * popularity


def _make_query(rng: random.Random, scale: str, index: int, items: list[dict]) -> dict:
    focus_category = rng.choice(CATEGORIES)
    vocab = CATEGORY_VOCAB[focus_category]
    focus_terms = rng.sample(vocab, k=min(2, len(vocab)))
    mixed_mode = index % 3
    if mixed_mode == 0:
        query_text = f"{focus_terms[0]} {focus_terms[1]}"
    elif mixed_mode == 1:
        other_category = rng.choice([category for category in CATEGORIES if category != focus_category])
        query_text = f"{focus_terms[0]} {rng.choice(CATEGORY_VOCAB[other_category])}"
    else:
        query_text = f"{focus_terms[0]} {focus_terms[1]} {focus_terms[0]}"
    if index % 5 == 0:
        query_text = f"{query_text} {rng.choice(('guide', '推荐', 'best', '新款'))}"

    history = [focus_category]
    while len(history) < rng.randint(2, 4):
        candidate = rng.choice(CATEGORIES)
        history.append(candidate)

    centroid = _category_centroid(focus_category)
    query_features = [centroid[dimension] + rng.gauss(0.0, 0.22) for dimension in range(FEATURE_DIM)]
    query = {
        "query_id": f"q-{scale}-{index:04d}",
        "query_text": query_text,
        "locale": rng.choice(("zh-CN", "en-US", "zh-CN", "en-US")),
        "channel": rng.choice(("search", "recommend", "search", "recommend", "discovery")),
        "history_categories": history,
        "query_features": _round_features(query_features),
    }
    scored = sorted(
        ((_relevance(item, query), item["item_id"]) for item in items),
        key=lambda pair: (-pair[0], pair[1]),
    )
    query["reference_item_ids"] = [item_id for _, item_id in scored[:10]]
    return query


def generate_dataset(out_dir: str | Path, *, seed: int = 20261120) -> Path:
    root = Path(out_dir)
    manifest_rows: list[dict] = []
    for spec in SCALES:
        scale_seed = seed + spec.seed_offset
        rng = random.Random(scale_seed)
        category_cycle = [category for category in CATEGORIES for _ in range(max(1, spec.item_count // len(CATEGORIES) + 1))]
        rng.shuffle(category_cycle)
        items = [
            _make_item(rng, spec.name, index, category_cycle[index % len(category_cycle)])
            for index in range(spec.item_count)
        ]
        queries = [_make_query(rng, spec.name, index, items) for index in range(spec.query_count)]
        _write_jsonl(root / spec.name / "items.jsonl", items)
        _write_jsonl(root / spec.name / "queries.jsonl", queries)
        manifest_rows.append(
            {
                "scale": spec.name,
                "items_file": f"{spec.name}/items.jsonl",
                "queries_file": f"{spec.name}/queries.jsonl",
                "item_count": len(items),
                "query_count": len(queries),
                "expected_candidate_depth": 240,
                "expected_fused_depth": 10,
                "seed": scale_seed,
            }
        )
    manifest_path = root / "manifest.jsonl"
    _write_jsonl(manifest_path, manifest_rows)
    return manifest_path
