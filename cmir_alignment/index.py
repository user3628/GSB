"""Per-shard dual inverted index over quantized shared-space vectors."""

from __future__ import annotations

from typing import Any, Iterable


def vector_index_keys(record: dict[str, Any], index_dims: int) -> list[str]:
    values = [float(value) for value in record["dequantized"]]
    order = sorted(
        range(len(values)),
        key=lambda index: (-abs(values[index]), index),
    )[:index_dims]
    keys = []
    for index in order:
        value = values[index]
        if abs(value) <= 1e-15:
            continue
        sign = "p" if value >= 0.0 else "n"
        keys.append(f"{sign}{index:02d}")
    return keys


def build_shard_index(
    projected_artifact: dict[str, Any],
    *,
    index_dims: int,
    projected_digest: str | None = None,
) -> dict[str, Any]:
    postings: dict[str, dict[str, list[str]]] = {"visual": {}, "text": {}}
    item_keys: dict[str, dict[str, list[str]]] = {}
    for item in projected_artifact["items"]:
        item_id = item["item_id"]
        visual_keys = vector_index_keys(item["visual"], index_dims)
        text_keys = vector_index_keys(item["text"], index_dims)
        item_keys[item_id] = {"visual": visual_keys, "text": text_keys}
        for component, keys in (("visual", visual_keys), ("text", text_keys)):
            for key in keys:
                postings[component].setdefault(key, [])
                if item_id not in postings[component][key]:
                    postings[component][key].append(item_id)
    for component in postings:
        for key in postings[component]:
            postings[component][key].sort()
    return {
        "schema": "cmir.shard-index.v1",
        "shard_id": projected_artifact["shard_id"],
        "projected_digest": projected_digest,
        "item_ids": sorted(item_keys),
        "index_dims": index_dims,
        "postings": postings,
        "item_keys": item_keys,
    }


def inverted_hits(
    indexes: Iterable[dict[str, Any]], query_keys: list[str]
) -> dict[str, int]:
    hits: dict[str, int] = {}
    unique_keys = sorted(set(query_keys))
    for index in indexes:
        postings = index["postings"]
        for key in unique_keys:
            for component in ("visual", "text"):
                for item_id in postings[component].get(key, []):
                    hits[item_id] = hits.get(item_id, 0) + 1
    return hits
