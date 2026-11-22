from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import Any, Iterable

from .errors import E_EMPTY_DOC, E_NO_TOKENS
from .normalize import (
    NORMALIZER_VERSION,
    TOKENIZER_VERSION,
    normalize_text,
    tokenize_normalized,
)
from .util import canonical_digest, sha256_text


HASH_FUNCTION_VERSION = "blake2b-person-txtnb01-v1"


def hash_term(term: str, hash_dim: int) -> int:
    digest = hashlib.blake2b(
        term.encode("utf-8"), digest_size=8, person=b"txtnb-01"
    ).digest()
    return int.from_bytes(digest, "big") % hash_dim


def extract_features(document: dict[str, Any], hash_dim: int) -> dict[str, Any]:
    doc_id = document["doc_id"]
    text = document.get("text", document.get("body", ""))
    normalized = normalize_text(text)
    if normalized == "":
        return {
            "batch_id": document["batch_id"],
            "doc_id": doc_id,
            "normalized_digest": sha256_text(normalized),
            "normalized_length": 0,
            "tokens": [],
            "token_count": 0,
            "hash_collision_count": 0,
            "collision_buckets": [],
            "error_code": E_EMPTY_DOC,
        }
    token_values = tokenize_normalized(normalized)
    bucket_terms: dict[int, set[str]] = defaultdict(set)
    output_tokens: list[dict[str, Any]] = []
    for position, token in enumerate(token_values):
        bucket = hash_term(token.term, hash_dim)
        bucket_terms[bucket].add(token.term)
        output_tokens.append(
            {
                "position": position,
                "term": token.term,
                "bucket": bucket,
                "start": token.start,
                "end": token.end,
            }
        )
    collision_buckets = sorted(bucket for bucket, terms in bucket_terms.items() if len(terms) > 1)
    collision_count = sum(len(bucket_terms[bucket]) - 1 for bucket in collision_buckets)
    error_code = E_NO_TOKENS if not output_tokens else None
    record: dict[str, Any] = {
        "batch_id": document["batch_id"],
        "doc_id": doc_id,
        "normalized_digest": sha256_text(normalized),
        "normalized_length": len(normalized),
        "tokens": output_tokens,
        "token_count": len(output_tokens),
        "hash_collision_count": collision_count,
        "collision_buckets": collision_buckets,
    }
    if error_code is not None:
        record["error_code"] = error_code
    return record


def feature_records_digest(records: Iterable[dict[str, Any]]) -> str:
    return canonical_digest(list(records))


def feature_input_fingerprint(
    batch_id: str,
    data_digest: str,
    row_count: int,
    hash_dim: int,
) -> str:
    return canonical_digest(
        {
            "stage_schema": "features-v1",
            "batch_id": batch_id,
            "data_digest": data_digest,
            "row_count": row_count,
            "hash_dim": hash_dim,
            "hash_function": HASH_FUNCTION_VERSION,
            "normalizer": NORMALIZER_VERSION,
            "tokenizer": TOKENIZER_VERSION,
        }
    )
