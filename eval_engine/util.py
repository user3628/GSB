"""Canonical serialization, hashing, fixed-point arithmetic, and atomic I/O."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Iterable, Sequence

SCORE_SCALE = 1_000_000
WEIGHT_SCALE = 1_000_000


def canonical_bytes(value: Any, *, newline: bool = True) -> bytes:
    """Serialize a JSON value deterministically as UTF-8 bytes."""

    text = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    if newline:
        text += "\n"
    return text.encode("utf-8")


def pretty_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        indent=2,
        allow_nan=False,
    )


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_json(value: Any) -> str:
    return sha256_bytes(canonical_bytes(value, newline=False))


def digest_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def fsync_directory(path: Path) -> None:
    """Best-effort directory fsync; Windows does not support directories this way."""

    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Write via a same-directory temporary file and os.replace."""

    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temp_path = Path(temp_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, path)
        fsync_directory(path.parent)
    except BaseException:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def atomic_write_json(path: Path, value: Any, *, pretty: bool = False) -> bytes:
    data = (
        (pretty_json(value) + "\n").encode("utf-8")
        if pretty
        else canonical_bytes(value)
    )
    atomic_write_bytes(path, data)
    return data


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def fixed_from_number(value: Any, scale: int = SCORE_SCALE) -> int:
    """Convert a JSON number to fixed point without binary-float arithmetic."""

    decimal_value = Decimal(str(value))
    return int(
        (decimal_value * Decimal(scale)).quantize(
            Decimal("1"), rounding=ROUND_HALF_UP
        )
    )


def fixed_to_string(value: int | None, scale: int = SCORE_SCALE) -> str | None:
    if value is None:
        return None
    whole, fraction = divmod(value, scale)
    return f"{whole}.{fraction:06d}"


def normalize_fixed_weights(
    values: Sequence[int], scale: int = WEIGHT_SCALE
) -> list[int]:
    """Normalize integer weights to exactly ``scale`` using largest remainder."""

    if not values:
        return []
    if any(value < 0 for value in values):
        raise ValueError("weights must be non-negative")
    total = sum(values)
    if total <= 0:
        raise ValueError("at least one weight must be positive")
    numerator = [value * scale for value in values]
    result = [number // total for number in numerator]
    remainder = scale - sum(result)
    if remainder:
        order = sorted(
            range(len(values)),
            key=lambda index: (-(numerator[index] % total), index),
        )
        for index in order[:remainder]:
            result[index] += 1
    return result


def rounded_ratio(numerator: int, denominator: int) -> int:
    if denominator <= 0:
        raise ValueError("denominator must be positive")
    return (numerator * 2 + denominator) // (2 * denominator)


def weighted_fixed_mean(means: Sequence[int], weights: Sequence[int]) -> int:
    if len(means) != len(weights) or not means:
        raise ValueError("means and normalized weights must be non-empty and aligned")
    total_weight = sum(weights)
    if total_weight <= 0:
        raise ValueError("total weight must be positive")
    numerator = sum(mean * weight for mean, weight in zip(means, weights))
    return rounded_ratio(numerator, total_weight)


_WHITESPACE_RE = re.compile(r"\s+", re.UNICODE)


def normalize_text(value: str | None) -> str:
    """NFKC, whitespace collapse, full/half-width unification, and lowercasing."""

    if value is None:
        return ""
    import unicodedata

    normalized = unicodedata.normalize("NFKC", str(value))
    normalized = _WHITESPACE_RE.sub(" ", normalized).strip()
    return normalized.lower()


_TOKEN_RE = re.compile(
    r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff"
    r"\u3040-\u30ff\uac00-\ud7af]|[^\W_]+",
    re.UNICODE,
)


def tokenize(value: str) -> list[str]:
    """Tokenize CJK as individual characters and other words as runs."""

    return _TOKEN_RE.findall(value)


def token_overlap_micros(prediction: str, reference: str) -> int:
    """Multiset token F1 expressed in millionths."""

    from collections import Counter

    prediction_tokens = tokenize(prediction)
    reference_tokens = tokenize(reference)
    if not prediction_tokens or not reference_tokens:
        return 0
    prediction_counts = Counter(prediction_tokens)
    reference_counts = Counter(reference_tokens)
    overlap = sum(
        min(prediction_counts[token], reference_counts[token])
        for token in prediction_counts.keys() & reference_counts.keys()
    )
    if overlap == 0:
        return 0
    denominator = len(prediction_tokens) + len(reference_tokens)
    return rounded_ratio(2 * overlap * SCORE_SCALE, denominator)


def stable_sorted_records(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        records,
        key=lambda record: (
            str(record.get("benchmark_id", "")),
            str(record.get("sample_id", "")),
            str(record.get("model_id", "")),
            str(record.get("run_id", "")),
        ),
    )
