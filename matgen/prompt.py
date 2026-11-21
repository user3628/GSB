"""Description normalization and deterministic control-vector generation."""

from __future__ import annotations

import hashlib
import math
import re
import struct
import unicodedata
from decimal import Decimal, localcontext
from typing import Any

from .canonical import sha256_bytes
from .errors import E_EMPTY_PROMPT, MatgenError
from .numeric import significant_digits


CONTROL_DIMENSIONS = 32
CONTROL_SCHEMA = "matgen.control.v1"
_TOKEN_PATTERN = re.compile(r"[^\W_]+", flags=re.UNICODE)
_WHITESPACE_PATTERN = re.compile(r"\s+", flags=re.UNICODE)
_SIGNED_SCALE = float(1 << 64)


def normalize_description(description: object, *, material_id: str | None) -> str:
    """NFKC-normalize, collapse whitespace, unify width forms, and lowercase."""

    if not isinstance(description, str):
        raise MatgenError(
            code=E_EMPTY_PROMPT,
            message="description must be a non-empty string",
            material_id=material_id,
            stage="control",
        )
    normalized = unicodedata.normalize("NFKC", description)
    normalized = _WHITESPACE_PATTERN.sub(" ", normalized).strip().lower()
    if not _TOKEN_PATTERN.search(normalized):
        raise MatgenError(
            code=E_EMPTY_PROMPT,
            message="description contains no letters or numbers",
            material_id=material_id,
            stage="control",
            details={"normalized": normalized},
        )
    return normalized


def tokenize_description(normalized: str) -> list[dict[str, Any]]:
    """Tokenize with Unicode word rules and aggregate integer token weights."""

    order: list[str] = []
    occurrences: dict[str, int] = {}
    for match in _TOKEN_PATTERN.finditer(normalized):
        token = match.group(0)
        if token not in occurrences:
            order.append(token)
            occurrences[token] = 0
        occurrences[token] += 1

    entries: list[dict[str, Any]] = []
    raw_weights = [occurrences[token] * max(1, len(token)) for token in order]
    total_weight = sum(raw_weights)
    for token, raw_weight in zip(order, raw_weights):
        entries.append(
            {
                "token": token,
                "occurrences": occurrences[token],
                "raw_weight": raw_weight,
                "normalized_weight": raw_weight / total_weight,
            }
        )
    return entries


def _signed_uniform(token: str, dimension: int, hash_seed: int) -> tuple[float, Decimal]:
    payload = f"matgen-control-v1\x1f{hash_seed}\x1f{token}\x1f{dimension}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    integer = int.from_bytes(digest[:8], "big")
    decimal_u = Decimal(integer) / Decimal(1 << 64)
    decimal_signed = decimal_u * Decimal(2) - Decimal(1)
    float_signed = (integer / _SIGNED_SCALE) * 2.0 - 1.0
    return float_signed, decimal_signed


def generate_control_vector(
    description: object,
    *,
    material_id: str | None,
    hash_seed: int,
) -> dict[str, Any]:
    """Generate a byte-reproducible 32-dimensional unit control vector."""

    normalized = normalize_description(description, material_id=material_id)
    tokens = tokenize_description(normalized)

    float_raw = [0.0] * CONTROL_DIMENSIONS
    decimal_raw = [Decimal(0)] * CONTROL_DIMENSIONS
    with localcontext() as context:
        context.prec = 60
        for token_entry in tokens:
            weight = token_entry["raw_weight"]
            decimal_weight = Decimal(weight)
            for dimension in range(CONTROL_DIMENSIONS):
                float_value, decimal_value = _signed_uniform(
                    token_entry["token"], dimension, hash_seed
                )
                float_raw[dimension] += float_value * weight
                decimal_raw[dimension] += decimal_value * decimal_weight

        float_norm = math.sqrt(math.fsum(component * component for component in float_raw))
        decimal_norm_square = sum(component * component for component in decimal_raw)
        decimal_norm = decimal_norm_square.sqrt()

    if float_norm <= 1e-30 or decimal_norm <= 0:
        raise MatgenError(
            code=E_EMPTY_PROMPT,
            message="normalized description produced a zero control vector",
            material_id=material_id,
            stage="control",
        )

    float_vector = [component / float_norm for component in float_raw]
    with localcontext() as context:
        context.prec = 60
        decimal_vector = [component / decimal_norm for component in decimal_raw]

    errors = [
        abs(float_component - float(decimal_component))
        for float_component, decimal_component in zip(float_vector, decimal_vector)
    ]
    vector_error_bound = max(errors) + 8.0 * math.ulp(1.0)
    float_norm_measured = math.sqrt(math.fsum(value * value for value in float_vector))
    norm_error = abs(float_norm_measured - 1.0)
    bound = max(vector_error_bound, norm_error + 8.0 * math.ulp(1.0))
    packed = struct.pack("<32d", *float_vector)
    return {
        "schema": CONTROL_SCHEMA,
        "material_id": material_id,
        "original_description": description,
        "normalized_description": normalized,
        "tokens": tokens,
        "hash_algorithm": "sha256-counter-v1",
        "hash_seed": hash_seed,
        "dimensions": CONTROL_DIMENSIONS,
        "control_vector": float_vector,
        "reference_vector_decimal": [str(component) for component in decimal_vector],
        "control_vector_bytes_sha256": sha256_bytes(packed),
        "norm": float_norm_measured,
        "norm_error": norm_error,
        "error_bound": bound,
        "relative_error_bound": bound,
        "significant_digits": significant_digits(1.0, bound),
        "reference_method": "decimal(60) token-weighted hash accumulation and sqrt",
    }


def control_fingerprint_input(recipe: dict[str, Any], hash_seed: int) -> dict[str, Any]:
    return {
        "material_id": recipe.get("material_id"),
        "description": recipe.get("description"),
        "hash_seed": hash_seed,
    }


def control_vector_bytes(control_artifact: dict[str, Any]) -> bytes:
    return struct.pack("<32d", *[float(value) for value in control_artifact["control_vector"]])

