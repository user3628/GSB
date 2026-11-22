"""Byte-deterministic serialization, hashing, fixed-point math and atomic IO."""
from __future__ import annotations

import hashlib
import io
import json
import os
import tempfile
from decimal import ROUND_HALF_UP, Decimal
from fractions import Fraction


def canonical_bytes(obj) -> bytes:
    """Serialize any supported object to canonical, stable UTF-8 JSON bytes."""
    return json.dumps(
        obj,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
        default=_default,
    ).encode("utf-8")


def canonical_text(obj) -> str:
    return canonical_bytes(obj).decode("utf-8")


def _default(obj):
    if isinstance(obj, Fraction):
        return {"__fraction__": "{}/{}".format(obj.numerator, obj.denominator)}
    if isinstance(obj, Decimal):
        return {"__decimal__": str(obj)}
    if isinstance(obj, set):
        return sorted(obj, key=lambda item: canonical_text(item))
    if hasattr(obj, "to_dict"):
        return obj.to_dict()
    raise TypeError("object of type %s is not canonical-serializable" % type(obj).__name__)


def digest(obj) -> str:
    """SHA-256 content digest over the canonical encoding of obj."""
    return hashlib.sha256(canonical_bytes(obj)).hexdigest()


def write_json(path, obj) -> None:
    """Atomically write obj as canonical JSON; no half-written file can remain."""
    path = os.fspath(path)
    directory = os.path.dirname(os.path.abspath(path))
    os.makedirs(directory, exist_ok=True)
    payload = canonical_bytes(obj)
    fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", suffix=".json", dir=directory)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        raise


def read_json(path):
    with open(os.fspath(path), "rb") as handle:
        return json.loads(handle.read().decode("utf-8"))


def quantize_fraction(value: Fraction, precision: int) -> int:
    """Round value * 10**precision to an int using banker-free half-up rounding."""
    scale = 10 ** precision
    scaled = Decimal(value.numerator * scale) / Decimal(value.denominator)
    return int(scaled.to_integral_value(rounding=ROUND_HALF_UP))


def fraction_from_text(text: str) -> Fraction:
    text = str(text).strip()
    if "/" in text:
        numerator, denominator = text.split("/", 1)
        return Fraction(int(numerator), int(denominator))
    return Fraction(text)


def compare_quantized(left: int, right: int) -> bool:
    return left == right


def stable_sorted(items, key):
    """Deterministic sort that tolerates heterogeneous key components."""
    return sorted(items, key=lambda item: _wrap_key(key(item)))


def _wrap_key(key_value):
    if isinstance(key_value, (tuple, list)):
        return tuple(_wrap_key(part) for part in key_value)
    if isinstance(key_value, dict):
        return (1, canonical_text(key_value))
    if key_value is None:
        return (0, "")
    if isinstance(key_value, bool):
        return (1, int(key_value))
    if isinstance(key_value, (int, float, str)):
        return (1, key_value)
    return (1, canonical_text(key_value))


def ensure_dir(path) -> str:
    os.makedirs(os.fspath(path), exist_ok=True)
    return os.fspath(path)


def open_text_buffer():
    return io.StringIO()
