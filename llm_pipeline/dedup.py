"""Unicode normalization and deterministic 64-bit SimHash deduplication."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import Counter, defaultdict
from typing import Any, Callable

import numpy as np

from .config import BANDS, BAND_BITS, NORMALIZE_ALGORITHM, SIMHASH_ALGORITHM
from .errors import E_EMPTY_TEXT
from .workspace import record_keep_product_bytes
from .util import sha256_bytes

_WHITESPACE_RE = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text)
    normalized = _WHITESPACE_RE.sub(" ", normalized)
    return normalized.strip().lower()


_TOKEN_RE = re.compile(
    r"[a-z0-9_]+|[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]"
)


def simhash_features(normalized_text: str) -> list[str]:
    """Stable lexical features for mixed Chinese/English text.

    Runs of Latin letters/digits become words; each CJK ideograph is one
    feature.  This makes a one-word or one-character edit a very small change
    while still producing a normal 64-bit SimHash.
    """
    return _TOKEN_RE.findall(normalized_text)


def simhash64(normalized_text: str) -> int:
    features = Counter(simhash_features(normalized_text))
    if not features:
        return 0
    vector = np.zeros(64, dtype=np.int64)
    for feature in sorted(features):
        weight = int(features[feature])
        digest = hashlib.blake2b(feature.encode("utf-8"), digest_size=8).digest()
        value = int.from_bytes(digest, "big")
        for bit in range(64):
            if (value >> bit) & 1:
                vector[bit] += weight
            else:
                vector[bit] -= weight
    result = 0
    for bit in range(64):
        if int(vector[bit]) > 0:
            result |= 1 << bit
    return result


def simhash_hex(value: int) -> str:
    return f"{value & ((1 << 64) - 1):016x}"


def hamming_distance(left: int, right: int) -> int:
    return int((left ^ right).bit_count())


def band_keys(simhash: int) -> list[tuple[int, int]]:
    mask = (1 << BAND_BITS) - 1
    return [
        (band, (simhash >> (band * BAND_BITS)) & mask)
        for band in range(BANDS)
    ]


def layer_totals(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    totals: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {"input_count": 0}
    )
    for record in records:
        key = (record["source"], record["lang"])
        totals[key]["input_count"] += 1
    return [
        {
            "source": source,
            "lang": lang,
            "layer_id": f"{source}::{lang}",
            "input_count": totals[(source, lang)]["input_count"],
        }
        for source, lang in sorted(totals)
    ]


def deduplicate(
    records: list[dict[str, Any]],
    config: dict[str, Any],
    *,
    check_stop: Callable[[], None] | None = None,
) -> dict[str, Any]:
    """Normalize and deduplicate records, returning a deterministic payload.

    An online greedy policy is used: records are visited by sample id, the
    first/lowest id becomes the representative, and candidates are only those
    sharing at least one 16-bit SimHash bucket with a retained representative.
    """
    threshold = int(config["dedup"]["hamming_threshold"])
    ordered = sorted(records, key=lambda item: item["id"])
    kept: list[dict[str, Any]] = []
    duplicates: list[dict[str, Any]] = []
    empty_errors: list[dict[str, Any]] = []
    kept_by_id: dict[str, dict[str, Any]] = {}
    index: dict[tuple[int, int], list[str]] = defaultdict(list)

    for record in ordered:
        normalized = normalize_text(record["text"])
        if normalized == "":
            empty_errors.append(
                {
                    "code": E_EMPTY_TEXT,
                    "id": record["id"],
                    "source": record["source"],
                    "lang": record["lang"],
                    "shard": record["_shard"],
                    "line": record["_line"],
                    "message": "text is empty after Unicode normalization and whitespace folding",
                }
            )
            if check_stop is not None:
                check_stop()
            continue

        value = simhash64(normalized)
        candidate_ids: set[str] = set()
        for key in band_keys(value):
            candidate_ids.update(index.get(key, ()))

        best_id: str | None = None
        best_distance: int | None = None
        for candidate_id in sorted(candidate_ids):
            candidate = kept_by_id[candidate_id]
            distance = hamming_distance(value, candidate["_simhash"])
            if distance <= threshold:
                if (
                    best_distance is None
                    or distance < best_distance
                    or (distance == best_distance and candidate_id < (best_id or ""))
                ):
                    best_id = candidate_id
                    best_distance = distance

        if best_id is not None:
            assert best_distance is not None
            representative = kept_by_id[best_id]
            duplicates.append(
                {
                    "id": record["id"],
                    "kept_id": best_id,
                    "distance": best_distance,
                    "normalized_equal": normalized == representative["normalized_text"],
                    "source": record["source"],
                    "lang": record["lang"],
                    "shard": record["_shard"],
                    "line": record["_line"],
                }
            )
            if check_stop is not None:
                check_stop()
            continue

        item = {
            "id": record["id"],
            "source": record["source"],
            "lang": record["lang"],
            "text": record["text"],
            "normalized_text": normalized,
            "simhash": simhash_hex(value),
            "shard": record["_shard"],
            "line": record["_line"],
            "_simhash": value,
        }
        kept.append(item)
        kept_by_id[record["id"]] = item
        for key in band_keys(value):
            index[key].append(record["id"])
        if check_stop is not None:
            check_stop()

    public_kept = [
        {key: value for key, value in item.items() if not key.startswith("_")}
        for item in kept
    ]
    payload: dict[str, Any] = {
        "algorithm": NORMALIZE_ALGORITHM,
        "simhash_algorithm": SIMHASH_ALGORITHM,
        "bands": BANDS,
        "hamming_threshold": threshold,
        "kept": public_kept,
        "duplicates": duplicates,
        "empty_errors": empty_errors,
        "observed_layers": layer_totals(records),
        "stats": {
            "input_count": len(records),
            "kept_count": len(public_kept),
            "duplicate_count": len(duplicates),
            "empty_error_count": len(empty_errors),
        },
    }
    payload["keep_list_digest"] = sha256_bytes(record_keep_product_bytes(payload))
    return payload
