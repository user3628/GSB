"""阶段二：多路改写候选生成（确定性、无外部服务）。"""

from __future__ import annotations

from typing import Any, Dict, List

from .config import Config
from .errors import E_UNKNOWN_TOKEN
from .sampling import glyph_code
from .util import fingerprint


def _round_conf(value: float) -> float:
    return round(float(value), 9)


def _replace_token(normalized: str, start: int, end: int, replacement: str) -> str:
    return normalized[:start] + replacement + normalized[end:]


# ---- 同义词路 -------------------------------------------------------------
def synonym_candidates(plan: Dict[str, Any], cfg: Config,
                       syn_table: Dict[str, Any]) -> List[Dict[str, Any]]:
    route = plan["routes"].get("synonym")
    if not route or not route.get("enabled"):
        return []
    max_cand = int(route["max_candidates"])
    lookup = {entry["term"]: entry for entry in syn_table["entries"]}
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for token in plan["tokens"]:
        entry = lookup.get(token["text"])
        if entry is None:
            continue
        for synonym in entry["synonyms"]:
            if len(out) >= max_cand:
                return _finalize(out, "synonym", seen)
            rewritten = _replace_token(plan["normalized_query"],
                                       token["start"], token["end"], synonym)
            if rewritten in seen or rewritten == plan["normalized_query"]:
                continue
            seen.add(rewritten)
            out.append({
                "route": "synonym",
                "rewritten_query": rewritten,
                "confidence": _round_conf(0.82),
                "basis": {
                    "type": "synonym_entry",
                    "term": token["text"],
                    "replacement": synonym,
                    "replaced_offset": [token["start"], token["end"]],
                    "entry_version": entry["entry_version"],
                    "dict_version": syn_table["version"],
                },
            })
    return _finalize(out, "synonym", seen)


# ---- 字形近似路 -----------------------------------------------------------
def glyph_candidates(plan: Dict[str, Any], cfg: Config,
                     glyph_index: Dict[str, Any]) -> List[Dict[str, Any]]:
    route = plan["routes"].get("glyph")
    if not route or not route.get("enabled"):
        return []
    max_cand = int(route["max_candidates"])
    seed = glyph_index["seed"]
    bucket_count = glyph_index["bucket_count"]
    buckets = {int(code): words for code, words in glyph_index["buckets"].items()}
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for token in plan["tokens"]:
        if token["kind"] != "latin":
            continue
        code = glyph_code(token["text"], seed, bucket_count)
        neighbors = sorted(w for w in buckets.get(code, []) if w != token["text"])
        for neighbor in neighbors:
            if len(out) >= max_cand:
                return _finalize(out, "glyph", seen)
            rewritten = _replace_token(plan["normalized_query"],
                                       token["start"], token["end"], neighbor)
            if rewritten in seen or rewritten == plan["normalized_query"]:
                continue
            seen.add(rewritten)
            # 近似路置信度低于同义路：同码命中但非同义
            out.append({
                "route": "glyph",
                "rewritten_query": rewritten,
                "confidence": _round_conf(0.55),
                "basis": {
                    "type": "glyph_bucket",
                    "term": token["text"],
                    "replacement": neighbor,
                    "replaced_offset": [token["start"], token["end"]],
                    "glyph_code": code,
                    "dict_version": glyph_index["version"],
                },
            })
    return _finalize(out, "glyph", seen)


# ---- 跨语言桥接路 ---------------------------------------------------------
def bridge_candidates(plan: Dict[str, Any], cfg: Config,
                      bridge_dict: Dict[str, Any]) -> List[Dict[str, Any]]:
    route = plan["routes"].get("bridge")
    if not route or not route.get("enabled"):
        return []
    max_cand = int(route["max_candidates"])
    mapping = bridge_dict["mapping"]
    marker_tpl = bridge_dict.get("unknown_marker", "[UNK:{token}]")
    target_lang = route.get("target_lang", "zh")
    out: List[Dict[str, Any]] = []
    seen: set[str] = set()
    unknown_events: List[Dict[str, Any]] = []

    for token in plan["tokens"]:
        hits = mapping.get(token["text"])
        if not hits:
            # 显式未知标记回退：绝不静默丢弃候选
            marker = marker_tpl.format(token=token["text"])
            unknown_events.append({
                "code": E_UNKNOWN_TOKEN,
                "term": token["text"],
                "fallback": marker,
                "offset": [token["start"], token["end"]],
            })
            rewritten = _replace_token(plan["normalized_query"],
                                       token["start"], token["end"], marker)
            if rewritten not in seen and rewritten != plan["normalized_query"] \
                    and len(out) < max_cand:
                seen.add(rewritten)
                out.append({
                    "route": "bridge",
                    "rewritten_query": rewritten,
                    "confidence": _round_conf(0.35),
                    "basis": {
                        "type": "unknown_marker_fallback",
                        "term": token["text"],
                        "replacement": marker,
                        "replaced_offset": [token["start"], token["end"]],
                        "target_lang": target_lang,
                        "dict_version": bridge_dict["version"],
                    },
                })
            continue

        for hit in hits:
            if len(out) >= max_cand:
                break
            if hit["target_lang"] != target_lang and len(hits) > 1:
                continue
            rewritten = _replace_token(plan["normalized_query"],
                                       token["start"], token["end"],
                                       hit["target"])
            if rewritten in seen or rewritten == plan["normalized_query"]:
                continue
            seen.add(rewritten)
            out.append({
                "route": "bridge",
                "rewritten_query": rewritten,
                "confidence": _round_conf(0.70),
                "basis": {
                    "type": "bridge_entry",
                    "term": token["text"],
                    "replacement": hit["target"],
                    "replaced_offset": [token["start"], token["end"]],
                    "target_lang": hit["target_lang"],
                    "dict_version": bridge_dict["version"],
                },
            })
    result = _finalize(out, "bridge", seen)
    if unknown_events:
        for cand in result:
            if cand["basis"]["type"] == "unknown_marker_fallback":
                cand["basis"]["unknown_events"] = unknown_events
    return result


def _finalize(out: List[Dict[str, Any]], route: str,
              seen: set[str]) -> List[Dict[str, Any]]:
    # 确定性排序：先按改写文本，再按 basis 指纹
    out.sort(key=lambda c: (c["rewritten_query"],
                            fingerprint(c["basis"])))
    for idx, cand in enumerate(out):
        cand["candidate_id"] = f"{route}-{idx:02d}"
        cand["candidate_fingerprint"] = fingerprint({
            "route": cand["route"],
            "rewritten_query": cand["rewritten_query"],
            "basis": cand["basis"],
        })
    return out


def generate_candidates(plan: Dict[str, Any], cfg: Config,
                        resources: Dict[str, Any]) -> Dict[str, Any]:
    """汇总三路候选，按 (route, candidate_id) 全局确定序排列。"""
    per_route = {
        "synonym": synonym_candidates(plan, cfg, resources["synonym"]),
        "glyph": glyph_candidates(plan, cfg, resources["glyph"]),
        "bridge": bridge_candidates(plan, cfg, resources["bridge"]),
    }
    candidates: List[Dict[str, Any]] = []
    for route in ("synonym", "glyph", "bridge"):
        candidates.extend(per_route[route])
    return {
        "query_id": plan["query_id"],
        "status": "candidates_ready",
        "candidates": candidates,
        "route_counts": {route: len(per_route[route])
                         for route in ("synonym", "glyph", "bridge")},
        "candidates_fingerprint": fingerprint(candidates),
    }
