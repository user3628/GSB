from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .errors import BusinessError, ErrorCode
from .util import digest_bytes, safe_relative_path


@dataclass(frozen=True)
class Item:
    item_id: str
    category: str
    title: str
    features: tuple[float, ...]


@dataclass(frozen=True)
class Query:
    query_id: str
    text: str
    locale: str
    channel: str
    history_categories: tuple[str, ...]
    features: tuple[float, ...]
    reference_item_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class ManifestEntry:
    scale: str
    items_file: str
    queries_file: str
    item_count: int
    query_count: int
    expected_candidate_depth: int
    seed: int


@dataclass(frozen=True)
class InputBundle:
    entry: ManifestEntry
    items: tuple[Item, ...]
    queries: tuple[Query, ...]
    items_digest: str
    queries_digest: str
    items_path: Path
    queries_path: Path

    @property
    def feature_dim(self) -> int:
        return len(self.items[0].features) if self.items else 0

    @property
    def content_digest(self) -> str:
        return digest_bytes(f"{self.items_digest}:{self.queries_digest}".encode("ascii"))


@dataclass(frozen=True)
class Manifest:
    path: Path
    entries: tuple[ManifestEntry, ...]
    digest: str

    def by_scale(self) -> dict[str, ManifestEntry]:
        return {entry.scale: entry for entry in self.entries}


def _read_jsonl(path: Path) -> tuple[list[dict[str, Any]], str]:
    try:
        data = path.read_bytes()
    except FileNotFoundError as exc:
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            "JSONL input file not found",
            {"path": str(path)},
        ) from exc
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            "JSONL input is not UTF-8",
            {"path": str(path), "reason": str(exc)},
        ) from exc
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise BusinessError(
                ErrorCode.INVALID_INPUT,
                "invalid JSONL row",
                {"path": str(path), "line": line_number, "reason": str(exc)},
            ) from exc
        if not isinstance(value, dict):
            raise BusinessError(
                ErrorCode.INVALID_INPUT,
                "JSONL row must be an object",
                {"path": str(path), "line": line_number},
            )
        rows.append(value)
    return rows, digest_bytes(data)


def _required_string(row: dict[str, Any], key: str, *, context: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value:
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            f"{context}.{key} must be a non-empty string",
            {"line_object": context},
        )
    return value


def _feature_list(value: Any, *, context: str) -> list[float]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise BusinessError(ErrorCode.INVALID_INPUT, f"{context} must be a numeric list")
    result: list[float] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise BusinessError(ErrorCode.INVALID_INPUT, f"{context} contains a non-number", {"value": item})
        result.append(float(item))
    return result


def _derive_query_features(query_id: str, text: str, history: Iterable[str], dim: int) -> tuple[float, ...]:
    if dim <= 0:
        return ()
    seed_material = f"{query_id}\x1f{text}\x1f{'|'.join(history)}".encode("utf-8")
    values: list[float] = []
    for index in range(dim):
        block = hashlib.sha256(seed_material + index.to_bytes(4, "big")).digest()[:8]
        integer = int.from_bytes(block, "big")
        values.append((integer / ((1 << 64) - 1)) * 2.0 - 1.0)
    return tuple(values)


def load_manifest(path: str | Path) -> Manifest:
    manifest_path = Path(path).resolve()
    rows, manifest_digest = _read_jsonl(manifest_path)
    if not rows:
        raise BusinessError(ErrorCode.INVALID_INPUT, "manifest has no scale entries", {"path": str(manifest_path)})
    entries: list[ManifestEntry] = []
    seen: set[str] = set()
    for row in rows:
        scale = _required_string(row, "scale", context="manifest")
        if scale in seen:
            raise BusinessError(ErrorCode.INVALID_INPUT, "duplicate scale in manifest", {"scale": scale})
        seen.add(scale)
        item_count = row.get("item_count", row.get("items"))
        query_count = row.get("query_count", row.get("queries"))
        if isinstance(item_count, bool) or not isinstance(item_count, int) or item_count < 0:
            raise BusinessError(ErrorCode.INVALID_INPUT, "manifest item_count must be non-negative", {"scale": scale})
        if isinstance(query_count, bool) or not isinstance(query_count, int) or query_count < 0:
            raise BusinessError(ErrorCode.INVALID_INPUT, "manifest query_count must be non-negative", {"scale": scale})
        expected_depth = row.get("expected_candidate_depth", 0)
        if isinstance(expected_depth, bool) or not isinstance(expected_depth, int) or expected_depth < 0:
            raise BusinessError(
                ErrorCode.INVALID_INPUT,
                "manifest expected_candidate_depth must be non-negative",
                {"scale": scale},
            )
        seed = row.get("seed", 0)
        if isinstance(seed, bool) or not isinstance(seed, int):
            raise BusinessError(ErrorCode.INVALID_INPUT, "manifest seed must be an integer", {"scale": scale})
        entries.append(
            ManifestEntry(
                scale=scale,
                items_file=_required_string(row, "items_file", context=f"manifest[{scale}]"),
                queries_file=_required_string(row, "queries_file", context=f"manifest[{scale}]"),
                item_count=item_count,
                query_count=query_count,
                expected_candidate_depth=expected_depth,
                seed=seed,
            )
        )
    return Manifest(path=manifest_path, entries=tuple(entries), digest=manifest_digest)


def load_input_bundle(manifest: Manifest, entry: ManifestEntry) -> InputBundle:
    items_path = (manifest.path.parent / safe_relative_path(entry.items_file)).resolve()
    queries_path = (manifest.path.parent / safe_relative_path(entry.queries_file)).resolve()
    item_rows, items_digest = _read_jsonl(items_path)
    query_rows, queries_digest = _read_jsonl(queries_path)
    if len(item_rows) != entry.item_count:
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            "manifest item_count does not match JSONL",
            {"scale": entry.scale, "declared": entry.item_count, "actual": len(item_rows)},
        )
    if len(query_rows) != entry.query_count:
        raise BusinessError(
            ErrorCode.INVALID_INPUT,
            "manifest query_count does not match JSONL",
            {"scale": entry.scale, "declared": entry.query_count, "actual": len(query_rows)},
        )

    items: list[Item] = []
    item_ids: set[str] = set()
    feature_dim: int | None = None
    for row in item_rows:
        item_id = _required_string(row, "item_id", context=f"items[{entry.scale}]")
        if item_id in item_ids:
            raise BusinessError(ErrorCode.INVALID_INPUT, "duplicate item_id", {"scale": entry.scale, "item_id": item_id})
        item_ids.add(item_id)
        category = _required_string(row, "category", context=f"item[{item_id}]")
        title = row.get("title", row.get("text", ""))
        if not isinstance(title, str):
            raise BusinessError(ErrorCode.INVALID_INPUT, "item.title must be a string", {"item_id": item_id})
        features = _feature_list(row.get("features", []), context=f"item[{item_id}].features")
        if feature_dim is None:
            feature_dim = len(features)
        elif len(features) != feature_dim:
            raise BusinessError(
                ErrorCode.INVALID_INPUT,
                "item feature dimensions differ",
                {"item_id": item_id, "expected": feature_dim, "actual": len(features)},
            )
        items.append(Item(item_id=item_id, category=category, title=title, features=tuple(features)))
    if not items:
        feature_dim = 0

    queries: list[Query] = []
    query_ids: set[str] = set()
    for row in query_rows:
        query_id = _required_string(row, "query_id", context=f"queries[{entry.scale}]")
        if query_id in query_ids:
            raise BusinessError(
                ErrorCode.INVALID_INPUT,
                "duplicate query_id",
                {"scale": entry.scale, "query_id": query_id},
            )
        query_ids.add(query_id)
        query_text = row.get("query_text", row.get("text"))
        if not isinstance(query_text, str):
            raise BusinessError(ErrorCode.INVALID_INPUT, "query_text must be a string", {"query_id": query_id})
        locale = row.get("locale", "")
        channel = row.get("channel", "")
        if not isinstance(locale, str) or not isinstance(channel, str):
            raise BusinessError(ErrorCode.INVALID_INPUT, "locale and channel must be strings", {"query_id": query_id})
        history_raw = row.get("history_categories", [])
        if not isinstance(history_raw, list) or any(not isinstance(value, str) for value in history_raw):
            raise BusinessError(
                ErrorCode.INVALID_INPUT,
                "history_categories must be a string list",
                {"query_id": query_id},
            )
        refs_raw = row.get("reference_item_ids", row.get("references", []))
        if not isinstance(refs_raw, list) or any(not isinstance(value, str) for value in refs_raw):
            raise BusinessError(
                ErrorCode.INVALID_INPUT,
                "reference_item_ids must be a string list",
                {"query_id": query_id},
            )
        features_value = _feature_list(row.get("query_features", row.get("features")), context=f"query[{query_id}].features")
        if features_value:
            if items and len(features_value) != feature_dim:
                raise BusinessError(
                    ErrorCode.INVALID_INPUT,
                    "query feature dimension differs from items",
                    {"query_id": query_id, "expected": feature_dim, "actual": len(features_value)},
                )
            features = tuple(features_value)
        else:
            features = _derive_query_features(query_id, query_text, history_raw, feature_dim)
        unknown_refs = sorted(set(refs_raw) - item_ids)
        if unknown_refs:
            raise BusinessError(
                ErrorCode.INVALID_INPUT,
                "reference_item_ids contains unknown items",
                {"query_id": query_id, "item_ids": unknown_refs[:10]},
            )
        queries.append(
            Query(
                query_id=query_id,
                text=query_text,
                locale=locale,
                channel=channel,
                history_categories=tuple(history_raw),
                features=features,
                reference_item_ids=tuple(refs_raw),
            )
        )

    return InputBundle(
        entry=entry,
        items=tuple(items),
        queries=tuple(queries),
        items_digest=items_digest,
        queries_digest=queries_digest,
        items_path=items_path,
        queries_path=queries_path,
    )


def load_all_inputs(manifest: Manifest) -> dict[str, InputBundle]:
    return {entry.scale: load_input_bundle(manifest, entry) for entry in manifest.entries}
