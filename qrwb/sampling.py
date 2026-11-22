"""固定随机种子本地样本生成器。

不访问任何外部服务：同义词表、字形近似桶、双语桥接词典与查询分片
全部由 :class:`random.Random` 配合显式种子确定性产出，相同种子与
配置必然生成逐字节一致的文件。
"""

from __future__ import annotations

import hashlib
import os
import random
from typing import Any, Dict, List, Tuple

from .config import Config
from .util import atomic_write_json, canonical_dumps, fingerprint

# 受控词池（中英对照，顺序固定，保证跨环境一致）
EN_WORDS = [
    "phone", "laptop", "shoes", "coffee", "camera", "book", "chair", "lamp",
    "jacket", "watch", "headphone", "monitor", "keyboard", "bottle",
    "umbrella", "guitar",
]
ZH_WORDS = [
    "手机", "笔记本", "鞋", "咖啡", "相机", "书", "椅子", "台灯",
    "夹克", "手表", "耳机", "显示器", "键盘", "水杯", "雨伞", "吉他",
]

# 中英文标点 / 全角字符混入，用于检验 NFKC 与全半角统一
MIX_TEMPLATES = [
    "{en}　{zh}",          # 全角空格
    "ｂｕｙ {zh}",         # 全角拉丁
    "{zh}  {en}",
    "购买 {zh} {en}",
    "best {en} 推荐",
    "{en} review {zh}",
    "{zh}{zh}",
    "{en} {en}",
    "  {zh}　",
]
PUNCT_ONLY = ["!!!", "……", "   ", "。。。", "***"]


# ---- 确定性资源生成 -------------------------------------------------------
def build_synonym_table(cfg: Config) -> Dict[str, Any]:
    section = cfg.raw["synonym"]
    rng = random.Random(section["seed"])
    pool = EN_WORDS + ZH_WORDS
    shuffled = pool[:]
    rng.shuffle(shuffled)
    group_size = int(section["group_size"])
    entries: List[Dict[str, Any]] = []
    size = min(int(section["table_size"]), len(pool))
    for idx in range(size):
        group = [shuffled[(idx * group_size + j) % len(shuffled)]
                 for j in range(group_size)]
        term = group[0]
        synonyms = [w for w in group[1:] if w != term]
        entries.append({
            "term": term,
            "synonyms": sorted(set(synonyms)),
            "entry_version": f"{cfg.raw['version']['dict_versions']['synonym']}-e{idx:03d}",
        })
    table = {
        "kind": "synonym_table",
        "version": cfg.raw["version"]["dict_versions"]["synonym"],
        "seed": section["seed"],
        "entries": entries,
    }
    return table


def glyph_code(token: str, seed: int, bucket_count: int) -> int:
    """固定哈希近似码：同码词项视为字形近似。"""
    digest = hashlib.sha256(f"glyph:{seed}:{token}".encode("utf-8")).hexdigest()
    return int(digest, 16) % bucket_count


def build_glyph_index(cfg: Config) -> Dict[str, Any]:
    section = cfg.raw["glyph"]
    buckets: Dict[int, List[str]] = {}
    vocab = sorted(set(EN_WORDS + ZH_WORDS))
    for token in vocab:
        code = glyph_code(token, section["seed"], section["bucket_count"])
        buckets.setdefault(code, []).append(token)
    index = {
        "kind": "glyph_index",
        "version": cfg.raw["version"]["dict_versions"]["glyph"],
        "seed": section["seed"],
        "bucket_count": section["bucket_count"],
        "buckets": {str(code): sorted(words) for code, words in sorted(buckets.items())},
    }
    return index


def build_bridge_dict(cfg: Config) -> Dict[str, Any]:
    section = cfg.raw["bridge"]
    rng = random.Random(section["seed"])
    pairs = list(zip(EN_WORDS, ZH_WORDS))
    rng.shuffle(pairs)
    mapping: Dict[str, List[Dict[str, str]]] = {}
    # 确定性地让部分词项缺失，触发 [UNK:token] 显式回退
    missing_selector = random.Random(section["seed"] + 999)
    missing_terms = {w for w in EN_WORDS if missing_selector.random() < 0.18}
    for en, zh in pairs:
        if en not in missing_terms:
            mapping.setdefault(en, []).append({"target": zh, "target_lang": "zh"})
        if zh not in {z for e, z in pairs if e in missing_terms}:
            mapping.setdefault(zh, []).append({"target": en, "target_lang": "en"})
    bridge = {
        "kind": "bridge_dictionary",
        "version": cfg.raw["version"]["dict_versions"]["bridge"],
        "seed": section["seed"],
        "unknown_marker": cfg.unknown_marker_template,
        "mapping": {k: sorted(v, key=lambda d: d["target"])
                    for k, v in sorted(mapping.items())},
        "intentionally_missing": sorted(missing_terms),
    }
    return bridge


# ---- 查询分片生成 ---------------------------------------------------------
def _make_query(rng: random.Random, shard: int, idx: int,
                forced_text: str | None = None) -> Dict[str, Any]:
    query_id = f"q{shard:02d}-{idx:04d}"
    if forced_text is not None:
        raw_query = forced_text
    else:
        roll = rng.random()
        if roll < 0.12:
            raw_query = rng.choice(PUNCT_ONLY)
        else:
            template = rng.choice(MIX_TEMPLATES)
            raw_query = template.format(en=rng.choice(EN_WORDS),
                                        zh=rng.choice(ZH_WORDS))
    # lang 声明为主语言；混排查询统一标 mixed
    has_zh = any("\u4e00" <= ch <= "\u9fff" for ch in raw_query)
    has_latin = any("a" <= ch.lower() <= "z" for ch in raw_query)
    lang = "mixed" if has_zh and has_latin else ("zh" if has_zh else
           ("en" if has_latin else "empty"))
    record: Dict[str, Any] = {
        "query_id": query_id,
        "raw_query": raw_query,
        "lang": lang,
        "locale": rng.choice(["zh-CN", "en-US", "zh-HK"]),
        "channel": rng.choice(["app", "web", "mini"]),
    }
    if rng.random() < 0.4:
        record["click_history"] = [rng.choice(EN_WORDS + ZH_WORDS)
                                   for _ in range(rng.randint(1, 2))]
    return record


def generate_shards(cfg: Config, shard_count: int = 2,
                    rows_per_shard: int = 12) -> Tuple[List[List[Dict[str, Any]]],
                                                        Dict[str, Any]]:
    rng = random.Random(9001 + shard_count * 100 + rows_per_shard)
    shards: List[List[Dict[str, Any]]] = []
    duplicate_of: Dict[str, str] = {}
    for shard in range(1, shard_count + 1):
        rows = [_make_query(rng, shard, idx) for idx in range(rows_per_shard)]
        shards.append(rows)
    # 强制跨分片重复：后续分片各复制一条首分片的原始查询文本（新 query_id）
    for shard in range(1, shard_count):
        source = shards[0][rng.randint(0, rows_per_shard - 1)]
        dup = _make_query(rng, shard + 1, rows_per_shard,
                          forced_text=source["raw_query"])
        dup["lang"] = source["lang"]
        dup["locale"] = source["locale"]
        dup["channel"] = source["channel"]
        shards[shard].append(dup)
        duplicate_of[dup["query_id"]] = source["query_id"]

    manifest_shards = []
    all_records: List[Dict[str, Any]] = []
    for idx, rows in enumerate(shards, start=1):
        corpus_fingerprint = fingerprint(rows)
        manifest_shards.append({
            "shard_id": f"shard-{idx:02d}",
            "path": f"shards/shard-{idx:02d}.jsonl",
            "row_count": len(rows),
            "corpus_fingerprint": corpus_fingerprint,
        })
        all_records.extend(rows)
    manifest = {
        "kind": "shard_manifest",
        "shard_count": shard_count,
        "shards": manifest_shards,
        "total_rows": len(all_records),
        "global_corpus_fingerprint": fingerprint(all_records),
        "duplicate_queries": [{"query_id": k, "same_as": v}
                              for k, v in sorted(duplicate_of.items())],
    }
    return shards, manifest


# ---- 落盘 -----------------------------------------------------------------
def generate_workspace_inputs(cfg: Config, input_dir: str,
                              shard_count: int = 2,
                              rows_per_shard: int = 12) -> Dict[str, str]:
    os.makedirs(input_dir, exist_ok=True)
    shard_dir = os.path.join(input_dir, "shards")
    os.makedirs(shard_dir, exist_ok=True)

    syn = build_synonym_table(cfg)
    glyph = build_glyph_index(cfg)
    bridge = build_bridge_dict(cfg)
    shards, manifest = generate_shards(cfg, shard_count, rows_per_shard)

    paths: Dict[str, str] = {}
    paths["synonym"] = os.path.join(input_dir, "synonym_table.json")
    paths["glyph"] = os.path.join(input_dir, "glyph_index.json")
    paths["bridge"] = os.path.join(input_dir, "bridge_dictionary.json")
    paths["manifest"] = os.path.join(input_dir, "manifest.json")
    atomic_write_json(paths["synonym"], syn)
    atomic_write_json(paths["glyph"], glyph)
    atomic_write_json(paths["bridge"], bridge)
    atomic_write_json(paths["manifest"], manifest)

    for idx, rows in enumerate(shards, start=1):
        shard_path = os.path.join(shard_dir, f"shard-{idx:02d}.jsonl")
        lines = [canonical_dumps(record) for record in rows]
        # JSONL 逐行确定性写入（atomic_write_text 保证不留半写）
        from .util import atomic_write_text
        atomic_write_text(shard_path, "\n".join(lines) + "\n")
        paths[f"shard-{idx:02d}"] = shard_path
    return paths
