from __future__ import annotations

import random
from collections import Counter
from pathlib import Path
from typing import Any

from .config import TIERS
from .tokenizer import base_token_texts
from .util import (
    canonical_bytes,
    ensure_relative_to,
    sha256_bytes,
    sha256_file,
    write_json_atomic,
    write_jsonl_atomic,
)

GENERATOR_VERSION = "synthetic-parallel-v1"
DEFAULT_SEED = 20261123


CONCEPTS: tuple[tuple[str, str, str], ...] = (
    ("the", "的", "det"),
    ("a", "一", "det"),
    ("red", "红", "adj"),
    ("blue", "蓝", "adj"),
    ("green", "绿", "adj"),
    ("small", "小", "adj"),
    ("big", "大", "adj"),
    ("fast", "快", "adj"),
    ("slow", "慢", "adj"),
    ("good", "好", "adj"),
    ("bad", "坏", "adj"),
    ("new", "新", "adj"),
    ("old", "旧", "adj"),
    ("cat", "猫", "noun"),
    ("dog", "狗", "noun"),
    ("bird", "鸟", "noun"),
    ("fish", "鱼", "noun"),
    ("car", "车", "noun"),
    ("train", "列", "noun"),
    ("book", "书", "noun"),
    ("house", "房", "noun"),
    ("city", "城", "noun"),
    ("teacher", "师", "noun"),
    ("student", "生", "noun"),
    ("music", "乐", "noun"),
    ("picture", "图", "noun"),
    ("market", "市", "noun"),
    ("station", "站", "noun"),
    ("computer", "机", "noun"),
    ("network", "网", "noun"),
    ("data", "数", "noun"),
    ("model", "模", "noun"),
    ("river", "河", "noun"),
    ("mountain", "山", "noun"),
    ("flower", "花", "noun"),
    ("tree", "树", "noun"),
    ("wind", "风", "noun"),
    ("rain", "雨", "noun"),
    ("cloud", "云", "noun"),
    ("light", "光", "noun"),
    ("road", "路", "noun"),
    ("bridge", "桥", "noun"),
    ("door", "门", "noun"),
    ("window", "窗", "noun"),
    ("table", "桌", "noun"),
    ("chair", "椅", "noun"),
    ("paper", "纸", "noun"),
    ("pen", "笔", "noun"),
    ("phone", "话", "noun"),
    ("radio", "音", "noun"),
    ("sea", "海", "noun"),
    ("sky", "天", "noun"),
    ("north", "北", "noun"),
    ("south", "南", "noun"),
    ("king", "王", "noun"),
    ("queen", "后", "noun"),
    ("child", "童", "noun"),
    ("friend", "友", "noun"),
    ("read", "读", "verb"),
    ("write", "写", "verb"),
    ("buy", "买", "verb"),
    ("sell", "卖", "verb"),
    ("go", "去", "verb"),
    ("come", "来", "verb"),
    ("open", "开", "verb"),
    ("close", "关", "verb"),
    ("see", "见", "verb"),
    ("make", "造", "verb"),
    ("find", "找", "verb"),
    ("keep", "存", "verb"),
    ("today", "今", "time"),
    ("tomorrow", "明", "time"),
    ("morning", "晨", "time"),
    ("night", "夜", "time"),
    ("happy", "喜", "adj"),
    ("sad", "悲", "adj"),
    ("hot", "热", "adj"),
    ("cold", "冷", "adj"),
    ("easy", "易", "adj"),
    ("hard", "难", "adj"),
    ("rich", "富", "adj"),
    ("poor", "贫", "adj"),
    ("long", "长", "adj"),
    ("short", "短", "adj"),
)

ACTIVE_CONCEPT_COUNTS = {"small": 36, "medium": 62, "large": 84}
DOMAINS = ("daily", "travel", "technology", "finance", "health")
LANG_PAIRS = ("en-zh", "zh-en")
TEMPLATES: tuple[tuple[str, ...], ...] = (
    ("det", "adj", "noun", "verb", "det", "noun"),
    ("noun", "verb", "adj", "noun", "time"),
    ("det", "noun", "verb", "time"),
    ("adj", "noun", "verb", "adj", "noun"),
    ("det", "adj", "noun", "verb", "noun", "time"),
    ("noun", "verb", "adj", "noun"),
    ("det", "noun", "verb", "det", "adj", "noun"),
)


def _active_concepts(tier: str) -> list[tuple[str, str, str]]:
    count = ACTIVE_CONCEPT_COUNTS[tier]
    active = list(CONCEPTS[:count])
    # The ordered prefix can cut one POS short at some tiers.  Fill each role
    # deterministically so every template remains usable.
    by_role: dict[str, list[tuple[str, str, str]]] = {}
    for concept in active:
        by_role.setdefault(concept[2], []).append(concept)
    for role in ("det", "adj", "noun", "verb", "time"):
        if not by_role.get(role):
            for concept in CONCEPTS:
                if concept[2] == role:
                    active.append(concept)
                    by_role.setdefault(role, []).append(concept)
                    break
    return active


def _sentence_concepts(rng: random.Random, tier: str) -> list[tuple[str, str, str]]:
    active = _active_concepts(tier)
    by_role: dict[str, list[tuple[str, str, str]]] = {}
    for concept in active:
        by_role.setdefault(concept[2], []).append(concept)
    template = TEMPLATES[rng.randrange(len(TEMPLATES))]
    return [by_role[role][rng.randrange(len(by_role[role]))] for role in template]


def _fullwidth_mix(text: str) -> str:
    chars: list[str] = []
    alpha_index = 0
    for char in text:
        if ("a" <= char <= "z") or ("0" <= char <= "9"):
            if alpha_index % 3 == 0:
                chars.append(chr(ord(char) + 0xFEE0))
            else:
                chars.append(char)
            alpha_index += 1
        elif char == ".":
            chars.append("．")
        else:
            chars.append(char)
    return "".join(chars)


def _insert_extra_spaces(text: str) -> str:
    if any(char.isspace() for char in text):
        pieces = text.split(" ")
        return "  " + "   ".join(pieces) + "  "
    if len(text) > 2:
        return text[:1] + "   " + text[1:2] + "  " + text[2:]
    return " " + text + " "


def _noise_flags(index: int) -> list[str]:
    flags: list[str] = []
    if index == 3 or index % 37 == 7:
        flags.append("extra_whitespace")
    if index == 11 or index % 43 == 13:
        flags.append("fullwidth_halfwidth_mix")
    if index == 17 or index % 71 == 41:
        flags.append("zero_length_ratio")
    if index == 23 or index % 83 == 61:
        flags.append("punctuation_only")
    return flags


def _render_pair(
    rng: random.Random,
    *,
    tier: str,
    index: int,
    pair_id: str,
    lang_pair: str,
) -> dict[str, Any]:
    concepts = _sentence_concepts(rng, tier)
    english_words = [concept[0] for concept in concepts]
    chinese_chars = [concept[1] for concept in concepts]
    if lang_pair == "en-zh":
        source_lang, target_lang = "en", "zh"
        source = " ".join(english_words) + "."
        target = "".join(chinese_chars) + "。"
    else:
        source_lang, target_lang = "zh", "en"
        source = "".join(chinese_chars) + "。"
        target = " ".join(english_words) + "."

    flags = _noise_flags(index)
    if "extra_whitespace" in flags:
        source = _insert_extra_spaces(source)
        target = _insert_extra_spaces(target)
    if "fullwidth_halfwidth_mix" in flags:
        if source_lang == "en":
            source = _fullwidth_mix(source)
        if target_lang == "en":
            target = _fullwidth_mix(target)
    if "punctuation_only" in flags:
        source = "..." if source_lang == "en" else "。。。"
    if "zero_length_ratio" in flags:
        if lang_pair == "en-zh":
            target = ""
        else:
            source = ""

    return {
        "pair_id": pair_id,
        "source_lang": source_lang,
        "target_lang": target_lang,
        "lang_pair": lang_pair,
        "source": source,
        "target": target,
        "domain": DOMAINS[index % len(DOMAINS)],
        "noise_flags": flags,
    }


def generate_tier(tier: str, *, seed: int, count: int) -> list[dict[str, Any]]:
    tier_seed = seed + {"small": 101, "medium": 202, "large": 303}[tier]
    rng = random.Random(tier_seed)
    rows: list[dict[str, Any]] = []
    width = 6
    for index in range(count):
        pair_id = f"{tier}-{index + 1:0{width}d}"
        lang_pair = LANG_PAIRS[index % len(LANG_PAIRS)]
        rows.append(
            _render_pair(
                rng,
                tier=tier,
                index=index,
                pair_id=pair_id,
                lang_pair=lang_pair,
            )
        )
    return rows


def _tier_manifest(root: Path, tier: str, rows: list[dict[str, Any]]) -> dict[str, Any]:
    source_vocab: Counter[str] = Counter()
    target_vocab: Counter[str] = Counter()
    source_lengths: list[int] = []
    target_lengths: list[int] = []
    lang_pair_counts: Counter[str] = Counter()
    domain_counts: Counter[str] = Counter()
    noise_counts: Counter[str] = Counter()
    for row in rows:
        source_tokens = base_token_texts(row["source"])
        target_tokens = base_token_texts(row["target"])
        source_vocab.update(source_tokens)
        target_vocab.update(target_tokens)
        source_lengths.append(len(source_tokens))
        target_lengths.append(len(target_tokens))
        lang_pair_counts[row["lang_pair"]] += 1
        domain_counts[row["domain"]] += 1
        for flag in row["noise_flags"]:
            noise_counts[flag] += 1

    corpus_path = root / "data" / "corpora" / f"{tier}.jsonl"
    reference_path = root / "data" / "references" / f"{tier}.jsonl"
    union_vocab = set(source_vocab) | set(target_vocab)
    total_tokens = sum(source_lengths) + sum(target_lengths)
    longest = max(source_lengths + target_lengths) if rows else 0
    manifest = {
        "schema_version": 1,
        "generator_version": GENERATOR_VERSION,
        "tier": tier,
        "pair_count": len(rows),
        "corpus_file": ensure_relative_to(corpus_path, root),
        "reference_file": ensure_relative_to(reference_path, root),
        "corpus_sha256": sha256_file(corpus_path),
        "reference_sha256": sha256_file(reference_path),
        "avg_source_tokens": round(sum(source_lengths) / len(rows), 6) if rows else 0.0,
        "avg_target_tokens": round(sum(target_lengths) / len(rows), 6) if rows else 0.0,
        "avg_token_length": round(
            (sum(source_lengths) + sum(target_lengths)) / (2 * len(rows)), 6
        )
        if rows
        else 0.0,
        "source_vocab_size": len(source_vocab),
        "target_vocab_size": len(target_vocab),
        "vocab_size_union": len(union_vocab),
        "total_token_upper_bound": total_tokens,
        "max_sentence_token_length": longest,
        "lang_pair_counts": dict(sorted(lang_pair_counts.items())),
        "domain_counts": dict(sorted(domain_counts.items())),
        "noise_counts": dict(sorted(noise_counts.items())),
    }
    return manifest


def generate_dataset(root: Path, *, seed: int = DEFAULT_SEED) -> dict[str, Any]:
    root = root.resolve()
    for directory in (
        root / "data" / "corpora",
        root / "data" / "references",
        root / "data" / "manifests",
    ):
        directory.mkdir(parents=True, exist_ok=True)

    counts = {"small": 200, "medium": 2000, "large": 12000}
    global_manifest: dict[str, Any] = {
        "schema_version": 1,
        "generator_version": GENERATOR_VERSION,
        "generator_seed": seed,
        "tiers": {},
    }
    for tier in TIERS:
        rows = generate_tier(tier, seed=seed, count=counts[tier])
        corpus_path = root / "data" / "corpora" / f"{tier}.jsonl"
        reference_path = root / "data" / "references" / f"{tier}.jsonl"
        write_jsonl_atomic(corpus_path, rows)
        references = [
            {
                "pair_id": row["pair_id"],
                "lang_pair": row["lang_pair"],
                "reference": row["target"],
            }
            for row in rows
        ]
        write_jsonl_atomic(reference_path, references)
        manifest = _tier_manifest(root, tier, rows)
        write_json_atomic(root / "data" / "manifests" / f"{tier}.json", manifest)
        global_manifest["tiers"][tier] = manifest

    write_json_atomic(root / "data" / "manifest.json", global_manifest)
    (root / "data" / "schema_version.txt").write_text("1\n", encoding="utf-8")
    return global_manifest


def load_dataset_manifest(root: Path) -> dict[str, Any]:
    import json

    with (root / "data" / "manifest.json").open("r", encoding="utf-8") as handle:
        return json.load(handle)


def dataset_fingerprint(manifest: dict[str, Any]) -> str:
    return sha256_bytes(canonical_bytes(manifest))

