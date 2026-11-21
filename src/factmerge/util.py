"""Deterministic serialization, decimal, Unicode and atomic-I/O helpers."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import unicodedata
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
from typing import Any, Iterable


DECIMAL_CONTEXT_PRECISION = 60


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def digest_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest_obj(value: Any) -> str:
    return digest_bytes(canonical_json(value).encode("utf-8"))


def digest_without(value: dict[str, Any], key: str) -> str:
    return digest_obj({k: v for k, v in value.items() if k != key})


def sha256_file(path: str | Path) -> str:
    h = hashlib.sha256()
    with Path(path).open("rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def atomic_write_bytes(path: str | Path, data: bytes) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=str(target.parent))
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, target)
        try:
            dir_fd = os.open(str(target.parent), os.O_RDONLY)
        except (OSError, AttributeError):
            dir_fd = None
        if dir_fd is not None:
            try:
                try:
                    os.fsync(dir_fd)
                except OSError:
                    pass
            finally:
                os.close(dir_fd)
    finally:
        if tmp.exists():
            tmp.unlink(missing_ok=True)


def atomic_write_json(path: str | Path, value: Any) -> None:
    atomic_write_bytes(path, (canonical_json(value) + "\n").encode("utf-8"))


def atomic_write_jsonl(path: str | Path, rows: Iterable[dict[str, Any]]) -> None:
    text = "".join(canonical_json(row) + "\n" for row in rows)
    atomic_write_bytes(path, text.encode("utf-8"))


def read_json(path: str | Path) -> Any:
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)


def read_jsonl(path: str | Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8") as fh:
        for line_no, line in enumerate(fh, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no}: invalid JSON: {exc.msg}") from exc
            if not isinstance(row, dict):
                raise ValueError(f"{path}:{line_no}: expected a JSON object")
            rows.append(row)
    return rows


def dec(value: str | int | Decimal) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def decimal_text(value: Decimal, strip_trailing_zeros: bool = True) -> str:
    with localcontext() as ctx:
        ctx.prec = DECIMAL_CONTEXT_PRECISION
        text = format(value, "f")
    if strip_trailing_zeros and "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in {"", "-0"}:
        return "0"
    return text


def quantize(value: Decimal, quantum: str | Decimal, rounding: str = "ROUND_HALF_EVEN") -> Decimal:
    mode = getattr(__import__("decimal"), rounding)
    with localcontext() as ctx:
        ctx.prec = DECIMAL_CONTEXT_PRECISION
        return value.quantize(dec(quantum), rounding=mode)


def decimal_places(quantum: str | Decimal) -> int:
    exponent = dec(quantum).as_tuple().exponent
    return max(0, -int(exponent))


def safe_decimal(value: Any, label: str) -> Decimal:
    try:
        result = dec(value)
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"{label} is not a decimal: {value!r}") from exc
    if not result.is_finite():
        raise ValueError(f"{label} must be finite")
    return result


def _nfkc_parts_with_map(text: str) -> tuple[str, list[tuple[int, int]]]:
    pieces: list[str] = []
    mapping: list[tuple[int, int]] = []
    for index, char in enumerate(text):
        normalized = unicodedata.normalize("NFKC", char)
        pieces.append(normalized)
        mapping.extend([(index, index + 1)] * len(normalized))
    return "".join(pieces), mapping


def _align_map(
    source: str, aligned: str, source_map: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    if source == aligned:
        return source_map
    from difflib import SequenceMatcher

    matcher = SequenceMatcher(a=source, b=aligned, autojunk=False)
    result: list[tuple[int, int]] = []
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            result.extend(source_map[i1:i2])
        elif tag == "replace":
            if i1 < i2:
                result.extend(source_map[i1:i2][: j2 - j1])
            if len(result) < j2:
                fallback = source_map[min(i1, len(source_map) - 1)] if source_map else (0, 0)
                result.extend([fallback] * (j2 - len(result)))
        elif tag == "insert":
            fallback = source_map[min(i1, len(source_map) - 1)] if source_map else (0, 0)
            result.extend([fallback] * (j2 - j1))
    if len(result) < len(aligned):
        fallback = source_map[-1] if source_map else (0, 0)
        result.extend([fallback] * (len(aligned) - len(result)))
    return result[: len(aligned)]


@dataclass(frozen=True)
class NormalizedText:
    original: str
    normalized: str
    map_to_source: tuple[tuple[int, int], ...]

    def source_span(self, start: int, end: int) -> tuple[int, int]:
        if start < 0 or end < start or end > len(self.normalized):
            raise ValueError(f"normalized span [{start},{end}) is out of range")
        if start == end:
            if not self.map_to_source:
                return (0, 0)
            if start < len(self.map_to_source):
                pos = self.map_to_source[start][0]
            else:
                pos = self.map_to_source[-1][1]
            return (pos, pos)
        parts = self.map_to_source[start:end]
        return (min(p[0] for p in parts), max(p[1] for p in parts))

    def span_dict(self, start: int, end: int, field: str) -> dict[str, Any]:
        source_start, source_end = self.source_span(start, end)
        return {
            "field": field,
            "text": self.normalized[start:end],
            "start": start,
            "end": end,
            "source_start": source_start,
            "source_end": source_end,
            "coordinate_system": "NFKC-normalized UTF-16-independent code-point offsets",
        }


_MINUS_VARIANTS = {"−": "-", "﹣": "-", "－": "-"}


def normalize_text(text: str, *, fold_whitespace: bool = True) -> NormalizedText:
    if not isinstance(text, str):
        text = "" if text is None else str(text)
    per_char, char_map = _nfkc_parts_with_map(text)
    whole = unicodedata.normalize("NFKC", text)
    mapping = _align_map(per_char, whole, char_map)

    output: list[str] = []
    out_map: list[tuple[int, int]] = []
    whitespace_start: int | None = None
    whitespace_end: int | None = None
    for char, span in zip(whole, mapping):
        fixed = _MINUS_VARIANTS.get(char, char)
        if fold_whitespace and fixed.isspace():
            if whitespace_start is None:
                whitespace_start, whitespace_end = span
            else:
                whitespace_end = max(whitespace_end, span[1])
            continue
        if whitespace_start is not None:
            if not output or output[-1] != " ":
                output.append(" ")
                out_map.append((whitespace_start, whitespace_end or whitespace_start + 1))
            whitespace_start = whitespace_end = None
        output.append(fixed)
        out_map.append(span)
    if whitespace_start is not None and output and output[-1] != " ":
        output.append(" ")
        out_map.append((whitespace_start, whitespace_end or whitespace_start + 1))

    return NormalizedText(text, "".join(output), tuple(out_map))


def normalize_key(text: str) -> str:
    return normalize_text(text).normalized.casefold().strip()


def compact_key(text: str) -> str:
    return "".join(ch for ch in normalize_key(text) if not ch.isspace())
