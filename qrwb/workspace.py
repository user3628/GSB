"""工作区输入装载：资源文件、JSONL 分片与分片清单一致性校验。"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Tuple

from .config import Config
from .util import fingerprint


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def load_workspace(workspace: str, cfg: Config
                   ) -> Tuple[Dict[str, Any], List[Dict[str, Any]],
                              Dict[str, Any]]:
    input_dir = os.path.join(workspace, "inputs")
    syn = load_json(os.path.join(input_dir, "synonym_table.json"))
    glyph = load_json(os.path.join(input_dir, "glyph_index.json"))
    bridge = load_json(os.path.join(input_dir, "bridge_dictionary.json"))
    manifest = load_json(os.path.join(input_dir, "manifest.json"))

    records: List[Dict[str, Any]] = []
    for shard in manifest["shards"]:
        shard_path = os.path.join(input_dir, shard["path"])
        rows = load_jsonl(shard_path)
        # 显式观测：行数与语料指纹必须与清单一致
        if len(rows) != shard["row_count"]:
            raise ValueError(
                f"{shard['shard_id']} 行数不一致: 清单 {shard['row_count']} "
                f"实际 {len(rows)}")
        actual_fp = fingerprint(rows)
        if actual_fp != shard["corpus_fingerprint"]:
            raise ValueError(
                f"{shard['shard_id']} 语料指纹不一致: 清单 "
                f"{shard['corpus_fingerprint']} 实际 {actual_fp}")
        records.extend(rows)

    if fingerprint(records) != manifest["global_corpus_fingerprint"]:
        raise ValueError("全局语料指纹与分片清单不一致")

    resources = {
        "synonym": syn,
        "glyph": glyph,
        "bridge": bridge,
        "fingerprints": {
            "synonym": fingerprint(syn),
            "glyph": fingerprint(glyph),
            "bridge": fingerprint(bridge),
        },
    }
    return resources, records, manifest
