from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from .errors import version_malformed


_VERSION_RE = re.compile(r"^[vV]?(\d+(?:[._-]\d+)*)$")


@dataclass(frozen=True)
class VersionInfo:
    raw: str
    segments: tuple[int, ...]

    @property
    def normalized_segments(self) -> tuple[int, ...]:
        segments = list(self.segments)
        while len(segments) > 1 and segments[-1] == 0:
            segments.pop()
        return tuple(segments)

    def compare(self, other: "VersionInfo") -> int:
        width = max(len(self.normalized_segments), len(other.normalized_segments), 1)
        left = self.normalized_segments + (0,) * (width - len(self.normalized_segments))
        right = other.normalized_segments + (0,) * (width - len(other.normalized_segments))
        return (left > right) - (left < right)

    def as_dict(self) -> dict[str, Any]:
        return {"raw": self.raw, "segments": list(self.segments)}


def parse_version(value: Any, field: str = "version") -> VersionInfo:
    if not isinstance(value, str):
        raise version_malformed(value, field)
    raw = value.strip()
    match = _VERSION_RE.fullmatch(raw)
    if not match:
        raise version_malformed(raw, field)
    segments = tuple(int(part) for part in re.split(r"[._-]", match.group(1)))
    if not segments:
        raise version_malformed(raw, field)
    return VersionInfo(raw=raw, segments=segments)


def relation(observed: VersionInfo, expected: VersionInfo) -> str:
    comparison = observed.compare(expected)
    if comparison < 0:
        return "BELOW"
    if comparison > 0:
        return "ABOVE"
    return "EQUAL"

