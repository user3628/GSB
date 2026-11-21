"""Segmented toolchain version parsing and ordering (e.g. ``2.1.0-rc3``).

Rules (deterministic, independent of input ordering):

* Numeric core segments are compared numerically (``2.10.0`` > ``2.9.0``).
* A pre-release segment (``-rc3``) ranks **below** the same core without a
  pre-release (``2.1.0-rc3`` < ``2.1.0``).
* Pre-release identifiers compare numerically when both are numeric,
  lexicographically otherwise; numeric identifiers rank below non-numeric.
* Missing core segments are treated as zero so unequal lengths still compare
  (``2.1`` == ``2.1.0``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import List, Tuple, Union

# Public tri-state labels.
LOWER = "lower"
EQUAL = "equal"
HIGHER = "higher"

_PreToken = Union[int, str]


@dataclass(frozen=True)
class ToolchainVersion:
    core: Tuple[int, ...]
    prerelease: Tuple[_PreToken, ...]  # empty tuple == release build

    @property
    def raw(self) -> str:
        core = ".".join(str(c) for c in self.core)
        if self.prerelease:
            return core + "-" + ".".join(str(p) for p in self.prerelease)
        return core

    def _sort_key(self) -> Tuple[Tuple[int, ...], int, Tuple[Tuple, ...]]:
        # Pad missing core segments with zero so 2.1 == 2.1.0.
        core = self.core + (0,) * (8 - len(self.core))
        # Release (no prerelease) sorts after any prerelease of the same core.
        pre_rank = 1 if not self.prerelease else 0
        tokens: List[Tuple] = []
        for token in self.prerelease:
            if isinstance(token, int):
                # numeric identifier ranks below alphanumeric, value compared numerically
                tokens.append((0, token))
            else:
                # natural ordering inside an alphanumeric identifier (rc10 > rc3)
                chunks = tuple(
                    (0, int(part)) if part.isdigit() else (1, part)
                    for part in re.findall(r"\d+|[^\d]+", token)
                )
                tokens.append((1, chunks))
        return (core, pre_rank, tuple(tokens))

    def __lt__(self, other: "ToolchainVersion") -> bool:
        return self._sort_key() < other._sort_key()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ToolchainVersion):
            return NotImplemented
        return self._sort_key() == other._sort_key()

    def __le__(self, other: "ToolchainVersion") -> bool:
        return self == other or self < other

    def __gt__(self, other: "ToolchainVersion") -> bool:
        return other < self

    def __ge__(self, other: "ToolchainVersion") -> bool:
        return self == other or other < self

    def __hash__(self) -> int:
        return hash(self._sort_key())


def parse_version(text: str) -> ToolchainVersion:
    """Parse a segmented version string into a comparable object."""
    text = text.strip()
    if not text:
        raise ValueError("empty version string")
    if "-" in text:
        core_text, pre_text = text.split("-", 1)
    else:
        core_text, pre_text = text, ""
    core_parts: List[int] = []
    for segment in core_text.split("."):
        if not segment.isdigit():
            raise ValueError(f"non-numeric core segment in version: {text!r}")
        core_parts.append(int(segment))
    if not core_parts:
        raise ValueError(f"no core segments in version: {text!r}")
    prerelease: List[_PreToken] = []
    if pre_text:
        for token in pre_text.split("."):
            if token.isdigit():
                prerelease.append(int(token))
            else:
                prerelease.append(token)
    return ToolchainVersion(tuple(core_parts), tuple(prerelease))


def compare_versions(left: str, right: str) -> str:
    """Return ``lower`` / ``equal`` / ``higher`` for ``left`` vs ``right``.

    The result is anti-symmetric: swapping inputs flips lower/higher, proving
    the comparison does not depend on input ordering.
    """
    lhs = parse_version(left)
    rhs = parse_version(right)
    if lhs == rhs:
        return EQUAL
    return HIGHER if lhs > rhs else LOWER
