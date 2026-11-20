from __future__ import annotations

import copy
import math
from pathlib import Path
from typing import Any, Iterable

from .errors import BusinessError, E_INPUT
from .normalize import normalize_text
from .util import atomic_write_json, digest_value, read_json


def load_bridge(path: Path) -> dict[str, Any]:
    try:
        value = read_json(path)
    except (OSError, ValueError) as exc:
        raise BusinessError(E_INPUT, f"cannot read bridge dictionary: {exc}") from exc
    validate_bridge(value)
    return value


def validate_bridge(bridge: dict[str, Any]) -> None:
    if not isinstance(bridge, dict) or not isinstance(bridge.get("entries"), list):
        raise BusinessError(E_INPUT, "bridge dictionary must contain an entries list")
    seen: set[str] = set()
    for entry in bridge["entries"]:
        if not isinstance(entry, dict):
            raise BusinessError(E_INPUT, "bridge entry must be an object")
        entry_id = entry.get("id")
        zh = entry.get("zh")
        en = entry.get("en")
        weight = entry.get("weight")
        if not isinstance(entry_id, str) or not entry_id:
            raise BusinessError(E_INPUT, "bridge entry id is required")
        if entry_id in seen:
            raise BusinessError(E_INPUT, f"duplicate bridge entry id: {entry_id}")
        seen.add(entry_id)
        if not isinstance(zh, str) or not normalize_text(zh):
            raise BusinessError(E_INPUT, f"bridge entry {entry_id} requires non-empty zh")
        if not isinstance(en, list) or not en or not all(isinstance(item, str) for item in en):
            raise BusinessError(E_INPUT, f"bridge entry {entry_id} requires non-empty en list")
        if (
            not isinstance(weight, (int, float))
            or isinstance(weight, bool)
            or not math.isfinite(float(weight))
        ):
            raise BusinessError(E_INPUT, f"bridge entry {entry_id} weight must be finite numeric")


def write_bridge(path: Path, bridge: dict[str, Any]) -> None:
    validate_bridge(bridge)
    atomic_write_json(path, bridge, indent=2)


def bridge_fingerprint(bridge: dict[str, Any]) -> str:
    return digest_value(bridge)


def _normalized_targets(entry: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for raw in entry["en"]:
        normalized = normalize_text(raw)
        if normalized and normalized not in values:
            values.append(normalized)
    return sorted(values)


def entry_terms(entry: dict[str, Any]) -> set[str]:
    terms = {normalize_text(entry["zh"])}
    for target in _normalized_targets(entry):
        terms.add(target)
        terms.update(target.split())
    terms.discard("")
    return terms


def relevant_bridge_view(
    bridge: dict[str, Any],
    token_terms: Iterable[str],
) -> dict[str, Any]:
    """Return only entries that could participate for a shard's token set."""
    terms = set(token_terms)
    chosen = [entry for entry in bridge["entries"] if entry_terms(entry) & terms]
    chosen.sort(key=lambda entry: entry["id"])
    return {
        "format_version": bridge.get("format_version", 1),
        "name": bridge.get("name", ""),
        "entries": copy.deepcopy(chosen),
    }


class BridgeIndex:
    def __init__(self, bridge: dict[str, Any]) -> None:
        validate_bridge(bridge)
        self.bridge = bridge
        self.entries = sorted(bridge["entries"], key=lambda entry: entry["id"])
        self._by_zh: dict[str, list[dict[str, Any]]] = {}
        self._by_en: dict[str, list[dict[str, Any]]] = {}
        for entry in self.entries:
            zh_value = normalize_text(entry["zh"])
            self._by_zh.setdefault(zh_value, []).append(entry)
            for target in _normalized_targets(entry):
                self._by_en.setdefault(target, []).append(entry)
                for token in target.split():
                    if token != target:
                        self._by_en.setdefault(token, []).append(entry)

    def targets_for_query_term(self, query_term: str) -> list[tuple[str, str, float]]:
        """Return (target_term, entry_id, entry_weight) candidates."""
        normalized = normalize_text(query_term)
        candidates: list[tuple[str, str, float]] = []
        for entry in self._by_zh.get(normalized, []):
            for target in _normalized_targets(entry):
                candidates.append((target, entry["id"], float(entry["weight"])))
                for token in target.split():
                    candidates.append((token, entry["id"], float(entry["weight"])))
        for entry in self._by_en.get(normalized, []):
            candidates.append((normalize_text(entry["zh"]), entry["id"], float(entry["weight"])))
        deduped: dict[tuple[str, str], tuple[str, str, float]] = {}
        for target, entry_id, weight in candidates:
            if not target:
                continue
            key = (target, entry_id)
            previous = deduped.get(key)
            if previous is None or weight > previous[2]:
                deduped[key] = (target, entry_id, weight)
        return sorted(deduped.values(), key=lambda item: (item[0], item[1], item[2]))


def edit_entry(
    bridge: dict[str, Any],
    entry_id: str,
    *,
    weight: float | None = None,
    en: list[str] | None = None,
    zh: str | None = None,
) -> dict[str, Any]:
    updated = copy.deepcopy(bridge)
    matches = [entry for entry in updated["entries"] if entry["id"] == entry_id]
    if not matches:
        raise BusinessError(E_INPUT, f"bridge entry not found: {entry_id}")
    entry = matches[0]
    if weight is not None:
        entry["weight"] = weight
    if en is not None:
        entry["en"] = en
    if zh is not None:
        entry["zh"] = zh
    validate_bridge(updated)
    return updated
