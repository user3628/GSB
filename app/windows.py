from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable

from .schemas import WindowSpec


@dataclass(frozen=True)
class WindowBounds:
    start_ms: int
    end_ms: int


def _ceil_div(value: int, divisor: int) -> int:
    return -((-value) // divisor)


def candidate_windows(spec: WindowSpec, timestamp_ms: int) -> list[WindowBounds]:
    """Return every deterministic window containing a timestamp."""
    if spec.kind == "tumbling":
        width = spec.width_ms
        assert width is not None
        start = (timestamp_ms // width) * width
        starts = [start]
        if spec.end_inclusive and timestamp_ms % width == 0:
            starts.insert(0, start - width)
        return [WindowBounds(s, s + width) for s in starts]

    if spec.kind == "sliding":
        width = spec.width_ms
        step = spec.step_ms
        assert width is not None and step is not None
        if spec.end_inclusive:
            k_min = _ceil_div(timestamp_ms - width, step)
        else:
            k_min = (timestamp_ms - width) // step + 1
        k_max = timestamp_ms // step
        starts = range(k_min, k_max + 1)
        return [WindowBounds(k * step, k * step + width) for k in starts]

    raise ValueError("session windows are assigned by session state, not timestamp algebra")


def make_window_id(spec_hash: str, kind: str, group_key: str, start_ms: int, end_ms: int | None = None) -> str:
    if kind == "session":
        material = f"{spec_hash}|session|{group_key}|{start_ms}"
        return "s_" + hashlib.sha256(material.encode()).hexdigest()[:24]
    material = f"{spec_hash}|{kind}|{group_key}|{start_ms}|{end_ms}"
    return "w_" + hashlib.sha256(material.encode()).hexdigest()[:24]


def contains(spec: WindowSpec, start_ms: int, end_ms: int, timestamp_ms: int) -> bool:
    if timestamp_ms < start_ms:
        return False
    if spec.end_inclusive:
        return timestamp_ms <= end_ms
    return timestamp_ms < end_ms


def overlaps(start_ms: int, end_ms: int, from_ms: int, to_ms: int) -> bool:
    return end_ms >= from_ms and start_ms <= to_ms


def overlap_count(spec: WindowSpec) -> int:
    if spec.kind == "sliding":
        assert spec.width_ms is not None and spec.step_ms is not None
        return spec.width_ms // spec.step_ms + (1 if spec.end_inclusive else 0)
    if spec.kind == "tumbling":
        return 2 if spec.end_inclusive else 1
    return 1


def unique_windows(windows: Iterable[WindowBounds]) -> list[WindowBounds]:
    seen: set[tuple[int, int]] = set()
    result: list[WindowBounds] = []
    for window in windows:
        key = (window.start_ms, window.end_ms)
        if key not in seen:
            seen.add(key)
            result.append(window)
    return result
