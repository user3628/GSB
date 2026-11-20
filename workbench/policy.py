from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable, Mapping

from .errors import wb_error

_HTTP_TOKEN = re.compile(r"^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")


def _invalid(message: str, **details: Any):
    return wb_error("E_SNAPSHOT_INVALID", message, **details)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def digest_snapshot_payload(payload: Mapping[str, Any]) -> str:
    cleaned = {
        key: value
        for key, value in payload.items()
        if key not in {"digest", "checksum", "snapshot_digest", "sha256"}
    }
    return hashlib.sha256(canonical_json(cleaned).encode("utf-8")).hexdigest()


def _normalized_digest(value: Any) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _invalid("Snapshot digest must be a non-empty string")
    normalized = value.strip().lower()
    if normalized.startswith("sha256:"):
        normalized = normalized.split(":", 1)[1]
    if not re.fullmatch(r"[0-9a-f]{64}", normalized):
        raise _invalid("Snapshot digest must be a SHA-256 hex digest")
    return normalized


def _require_number(payload: Mapping[str, Any], key: str, *, minimum: float = 0.0) -> float:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _invalid(f"Policy field '{key}' must be a number")
    numeric = float(value)
    if numeric < minimum:
        raise _invalid(f"Policy field '{key}' must be >= {minimum}")
    return numeric


@dataclass(frozen=True, slots=True)
class CachePolicy:
    version: int
    generated_at: str
    nodes: tuple[str, ...]
    ttl_seconds: float
    stale_while_revalidate_seconds: float
    cache_control: str
    vary: tuple[str, ...]
    purge_propagation_window_ms: float
    canonicalization: dict[str, bool]
    default_tags: tuple[str, ...]
    digest: str
    normalized_snapshot: dict[str, Any]

    @property
    def stale_window_seconds(self) -> float:
        return self.stale_while_revalidate_seconds

    @classmethod
    def from_snapshot(cls, payload: Mapping[str, Any], known_nodes: Iterable[str]) -> "CachePolicy":
        if not isinstance(payload, Mapping):
            raise _invalid("Snapshot must be a JSON object")
        version = payload.get("version")
        if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
            raise _invalid("Snapshot version must be a positive integer")
        generated_at = payload.get("generated_at")
        if not isinstance(generated_at, str) or not generated_at.strip():
            raise _invalid("Snapshot generated_at is required")
        try:
            datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise _invalid("Snapshot generated_at must be ISO-8601") from exc

        raw_nodes = payload.get("nodes")
        if not isinstance(raw_nodes, list) or not raw_nodes:
            raise _invalid("Snapshot nodes must be a non-empty list")
        nodes: list[str] = []
        for item in raw_nodes:
            node_id = item.get("id") if isinstance(item, Mapping) else item
            if not isinstance(node_id, str) or not node_id.strip():
                raise _invalid("Every snapshot node must have a non-empty id")
            node_id = node_id.strip()
            if node_id not in nodes:
                nodes.append(node_id)
        known = set(known_nodes)
        unknown = sorted(set(nodes) - known)
        if unknown:
            raise _invalid("Snapshot references unknown nodes", nodes=unknown)

        ttl_payload = dict(payload)
        if "ttl_seconds" not in ttl_payload and "ttl" in ttl_payload:
            ttl_payload["ttl_seconds"] = ttl_payload["ttl"]
        swr_payload = dict(payload)
        if "stale_while_revalidate_seconds" not in swr_payload:
            for alias in ("stale_while_revalidate", "swr_seconds"):
                if alias in swr_payload:
                    swr_payload["stale_while_revalidate_seconds"] = swr_payload[alias]
                    break
        ttl = _require_number(ttl_payload, "ttl_seconds", minimum=0.0)
        swr = _require_number(swr_payload, "stale_while_revalidate_seconds", minimum=0.0)
        cache_control = payload.get("cache_control", "")
        if not isinstance(cache_control, str) or not cache_control.strip():
            raise _invalid("Snapshot cache_control must be a non-empty string")
        if any(char in cache_control for char in "\r\n"):
            raise _invalid("Snapshot cache_control contains a newline")

        raw_vary = payload.get("vary", [])
        if not isinstance(raw_vary, list):
            raise _invalid("Snapshot vary must be a list")
        vary: list[str] = []
        for item in raw_vary:
            if not isinstance(item, str) or not item.strip():
                raise _invalid("Snapshot Vary names must be non-empty strings")
            name = item.strip().lower()
            if name == "*":
                raise _invalid("Vary '*' is not supported by the cache model")
            if not _HTTP_TOKEN.fullmatch(name):
                raise _invalid("Snapshot Vary contains an invalid HTTP header name", name=item)
            if name not in vary:
                vary.append(name)

        raw_purge = payload.get("purge", payload.get("purge_propagation", {}))
        if not isinstance(raw_purge, Mapping):
            raise _invalid("Snapshot purge policy must be an object")
        purge_window = raw_purge.get(
            "propagation_window_ms",
            payload.get("purge_propagation_window_ms", 1000),
        )
        if isinstance(purge_window, bool) or not isinstance(purge_window, (int, float)) or purge_window <= 0:
            raise _invalid("Purge propagation_window_ms must be a positive number")
        if raw_purge.get("strategy", "atomic") != "atomic":
            raise _invalid("Only the atomic purge propagation strategy is supported")

        raw_canonical = payload.get("canonicalization", {})
        if not isinstance(raw_canonical, Mapping):
            raise _invalid("Snapshot canonicalization must be an object")
        canonicalization = {
            "collapse_duplicate_slashes": bool(raw_canonical.get("collapse_duplicate_slashes", True)),
            "merge_dot_segments": bool(raw_canonical.get("merge_dot_segments", True)),
            "normalize_query": bool(raw_canonical.get("normalize_query", True)),
            "uppercase_percent_encoding": bool(raw_canonical.get("uppercase_percent_encoding", True)),
        }
        disabled = [name for name, enabled in canonicalization.items() if not enabled]
        if disabled:
            raise _invalid("Required canonicalization rules cannot be disabled", rules=disabled)

        raw_tags = payload.get("default_tags", [])
        if not isinstance(raw_tags, list) or any(not isinstance(tag, str) or not tag for tag in raw_tags):
            raise _invalid("default_tags must be a list of non-empty strings")
        default_tags = tuple(dict.fromkeys(raw_tags))

        normalized: dict[str, Any] = {
            "version": version,
            "generated_at": generated_at,
            "nodes": nodes,
            "ttl_seconds": ttl,
            "stale_while_revalidate_seconds": swr,
            "cache_control": cache_control.strip(),
            "vary": vary,
            "purge": {
                "strategy": "atomic",
                "propagation_window_ms": float(purge_window),
            },
            "canonicalization": canonicalization,
            "default_tags": list(default_tags),
        }
        computed_digest = digest_snapshot_payload(normalized)
        supplied_digest = payload.get("digest", payload.get("checksum"))
        if supplied_digest is None:
            supplied_digest = payload.get("snapshot_digest", payload.get("sha256"))
        if supplied_digest is not None:
            supplied = _normalized_digest(supplied_digest)
            raw_payload_digest = digest_snapshot_payload(payload)
            if supplied not in {computed_digest, raw_payload_digest}:
                raise _invalid(
                    "Snapshot digest verification failed",
                    computed_digest=computed_digest,
                    raw_payload_digest=raw_payload_digest,
                )
        normalized["digest"] = computed_digest
        return cls(
            version=version,
            generated_at=generated_at,
            nodes=tuple(nodes),
            ttl_seconds=ttl,
            stale_while_revalidate_seconds=swr,
            cache_control=normalized["cache_control"],
            vary=tuple(vary),
            purge_propagation_window_ms=float(purge_window),
            canonicalization=canonicalization,
            default_tags=default_tags,
            digest=computed_digest,
            normalized_snapshot=normalized,
        )

    def as_dict(self) -> dict[str, Any]:
        return dict(self.normalized_snapshot)

    def applies_to(self, node_id: str) -> bool:
        return node_id in self.nodes

