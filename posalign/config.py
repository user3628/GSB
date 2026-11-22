"""Governance configuration loading, validation, and stable fingerprints."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .canonical import digest_json, read_bytes, sha256_bytes
from .errors import BusinessError, E_INVALID_CONFIG
from .textnorm import normalize_label


STRATEGY_NAMES = {"frequency", "annotator_reliability", "candidate_priority"}


def _as_mapping(value: Any, name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise BusinessError(E_INVALID_CONFIG, f"{name} must be a mapping")
    return value


def _as_string_list(value: Any, name: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise BusinessError(E_INVALID_CONFIG, f"{name} must be a list of strings")
    return list(value)


@dataclass(frozen=True)
class AlignmentRules:
    min_rate: float = 0.75
    token_comparison: str = "nfkc_casefold"
    allow_unpaired: bool = True

    def as_dict(self) -> dict[str, Any]:
        return {
            "min_rate": self.min_rate,
            "token_comparison": self.token_comparison,
            "allow_unpaired": self.allow_unpaired,
        }


@dataclass(frozen=True)
class AdjudicationRules:
    strategy_order: tuple[str, ...]
    weights: dict[str, float]
    candidate_priority: tuple[str, ...]
    annotator_reliability: dict[str, float]
    default_annotator_reliability: float
    confidence_threshold: float
    tie_breaker: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "strategy_order": list(self.strategy_order),
            "weights": dict(sorted(self.weights.items())),
            "candidate_priority": list(self.candidate_priority),
            "annotator_reliability": dict(sorted(self.annotator_reliability.items())),
            "default_annotator_reliability": self.default_annotator_reliability,
            "confidence_threshold": self.confidence_threshold,
            "tie_breaker": self.tie_breaker,
        }


@dataclass(frozen=True)
class V1Rules:
    confidence_scale: str = "percent"
    zero_tokens: str = "unsegmented"
    preserve_original: bool = True

    def as_dict(self) -> dict[str, Any]:
        return {
            "confidence_scale": self.confidence_scale,
            "zero_tokens": self.zero_tokens,
            "preserve_original": self.preserve_original,
        }


@dataclass(frozen=True)
class GovernanceConfig:
    version: str
    standard_tags: tuple[str, ...]
    standard_lookup: dict[str, str]
    aliases: dict[str, tuple[str, ...]]
    standard_casefold: bool
    stem_separators: tuple[str, ...]
    alignment: AlignmentRules
    adjudication: AdjudicationRules
    v1: V1Rules
    source_path: str
    source_digest: str

    def semantic_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "standard_tags": list(self.standard_tags),
            "aliases": {key: list(value) for key, value in sorted(self.aliases.items())},
            "candidate_rules": {
                "standard_casefold": self.standard_casefold,
                "stem_separators": list(self.stem_separators),
            },
            "alignment": self.alignment.as_dict(),
            "adjudication": self.adjudication.as_dict(),
            "structure_v1": self.v1.as_dict(),
        }

    @property
    def alias_fingerprint(self) -> str:
        return digest_json(
            {
                "standard_tags": list(self.standard_tags),
                "aliases": {key: list(value) for key, value in sorted(self.aliases.items())},
            }
        )

    @property
    def alignment_fingerprint(self) -> str:
        return digest_json({"alignment": self.alignment.as_dict()})

    @property
    def adjudication_fingerprint(self) -> str:
        return digest_json({"adjudication": self.adjudication.as_dict()})

    @property
    def v1_fingerprint(self) -> str:
        return digest_json({"structure_v1": self.v1.as_dict()})

    @property
    def candidate_rules_fingerprint(self) -> str:
        return digest_json(
            {
                "standard_casefold": self.standard_casefold,
                "stem_separators": list(self.stem_separators),
            }
        )

    @property
    def semantic_digest(self) -> str:
        return digest_json(self.semantic_dict())

    def components(self) -> dict[str, str]:
        return {
            "governance_version": self.version,
            "semantic_digest": self.semantic_digest,
            "alias_fingerprint": self.alias_fingerprint,
            "alignment_fingerprint": self.alignment_fingerprint,
            "adjudication_fingerprint": self.adjudication_fingerprint,
            "v1_fingerprint": self.v1_fingerprint,
            "candidate_rules_fingerprint": self.candidate_rules_fingerprint,
        }


def _parse_yaml(text: str) -> Any:
    try:
        import yaml  # type: ignore
    except ImportError:
        from .yamlmini import safe_load

        return safe_load(text)
    try:
        return yaml.safe_load(text)
    except Exception as exc:
        raise BusinessError(
            E_INVALID_CONFIG,
            "invalid YAML configuration",
            {"cause": str(exc)},
        ) from exc


def _normalize_alias_table(
    raw_aliases: dict[str, Any],
    standard_lookup: dict[str, str],
) -> dict[str, tuple[str, ...]]:
    merged: dict[str, set[str]] = {}
    for raw_key, raw_candidates in raw_aliases.items():
        if not isinstance(raw_key, str):
            raise BusinessError(E_INVALID_CONFIG, "alias keys must be strings")
        candidates = _as_string_list(raw_candidates, f"aliases.{raw_key}")
        if not candidates:
            raise BusinessError(
                E_INVALID_CONFIG,
                f"alias {raw_key!r} must have at least one candidate",
            )
        key = normalize_label(raw_key)
        if not key:
            raise BusinessError(E_INVALID_CONFIG, "alias keys must not normalize to empty")
        canonical: set[str] = merged.setdefault(key, set())
        for candidate in candidates:
            tag = standard_lookup.get(normalize_label(candidate))
            if tag is None:
                raise BusinessError(
                    E_INVALID_CONFIG,
                    f"alias {raw_key!r} references unknown standard tag {candidate!r}",
                    {"alias": raw_key, "candidate": candidate},
                )
            canonical.add(tag)
    return {key: tuple(sorted(values)) for key, values in sorted(merged.items())}


def load_governance_config(path: str | Path) -> GovernanceConfig:
    config_path = Path(path).expanduser().resolve()
    raw_bytes = read_bytes(config_path)
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise BusinessError(
            E_INVALID_CONFIG,
            "governance config is not valid UTF-8",
            {"path": str(config_path)},
        ) from exc
    data = _parse_yaml(text)
    if data is None:
        data = {}
    if not isinstance(data, dict):
        raise BusinessError(E_INVALID_CONFIG, "governance config root must be a mapping")

    version = data.get("version", "gov-1")
    if not isinstance(version, str) or not version:
        raise BusinessError(E_INVALID_CONFIG, "version must be a non-empty string")
    standard_tags = _as_string_list(data.get("standard_tags"), "standard_tags")
    if not standard_tags:
        raise BusinessError(E_INVALID_CONFIG, "standard_tags must not be empty")
    standard_lookup: dict[str, str] = {}
    canonical_tags: list[str] = []
    for raw_tag in standard_tags:
        tag = raw_tag.strip()
        key = normalize_label(tag)
        if not tag or not key:
            raise BusinessError(E_INVALID_CONFIG, "standard tags must not be empty")
        if key in standard_lookup:
            raise BusinessError(
                E_INVALID_CONFIG,
                f"standard tags collide after normalization: {tag!r}",
                {"normalized": key},
            )
        standard_lookup[key] = tag
        canonical_tags.append(tag)

    aliases = _normalize_alias_table(_as_mapping(data.get("aliases"), "aliases"), standard_lookup)
    candidate_rules = _as_mapping(data.get("candidate_rules"), "candidate_rules")
    standard_casefold = bool(candidate_rules.get("standard_casefold", True))
    separators_raw = candidate_rules.get(
        "stem_separators", ["+", "/", "@", ":", "-", "_"]
    )
    stem_separators = _as_string_list(separators_raw, "candidate_rules.stem_separators")
    if any(not sep for sep in stem_separators):
        raise BusinessError(E_INVALID_CONFIG, "stem separators must be non-empty")

    alignment_raw = _as_mapping(data.get("alignment"), "alignment")
    min_rate = float(alignment_raw.get("min_rate", 0.75))
    if not 0.0 <= min_rate <= 1.0:
        raise BusinessError(E_INVALID_CONFIG, "alignment.min_rate must be in [0, 1]")
    token_comparison = str(alignment_raw.get("token_comparison", "nfkc_casefold"))
    if token_comparison not in {"nfkc_casefold", "exact"}:
        raise BusinessError(
            E_INVALID_CONFIG,
            "alignment.token_comparison must be nfkc_casefold or exact",
        )
    allow_unpaired = bool(alignment_raw.get("allow_unpaired", True))

    adj_raw = _as_mapping(data.get("adjudication"), "adjudication")
    strategy_order = tuple(
        _as_string_list(
            adj_raw.get(
                "strategy_order",
                ["frequency", "annotator_reliability", "candidate_priority"],
            ),
            "adjudication.strategy_order",
        )
    )
    if not strategy_order or len(set(strategy_order)) != len(strategy_order):
        raise BusinessError(E_INVALID_CONFIG, "adjudication.strategy_order must be unique")
    unknown_strategies = set(strategy_order) - STRATEGY_NAMES
    if unknown_strategies:
        raise BusinessError(
            E_INVALID_CONFIG,
            "unknown adjudication strategy",
            {"unknown": sorted(unknown_strategies)},
        )
    raw_weights = _as_mapping(adj_raw.get("weights"), "adjudication.weights")
    default_weight = 1.0 / len(strategy_order)
    weights = {
        name: float(raw_weights.get(name, default_weight)) for name in strategy_order
    }
    if any(weight < 0 for weight in weights.values()) or sum(weights.values()) <= 0:
        raise BusinessError(E_INVALID_CONFIG, "adjudication weights must be non-negative")
    weight_sum = sum(weights.values())
    weights = {name: value / weight_sum for name, value in weights.items()}
    candidate_priority = tuple(
        _as_string_list(adj_raw.get("candidate_priority", []), "adjudication.candidate_priority")
    )
    for raw_tag in candidate_priority:
        if normalize_label(raw_tag) not in standard_lookup:
            raise BusinessError(
                E_INVALID_CONFIG,
                f"unknown candidate priority tag: {raw_tag}",
            )
    reliability_raw = _as_mapping(
        adj_raw.get("annotator_reliability"), "adjudication.annotator_reliability"
    )
    reliability: dict[str, float] = {}
    for annotator, value in reliability_raw.items():
        if not isinstance(annotator, str):
            raise BusinessError(E_INVALID_CONFIG, "annotator ids must be strings")
        score = float(value)
        if not 0.0 <= score <= 1.0:
            raise BusinessError(
                E_INVALID_CONFIG,
                f"annotator reliability for {annotator!r} must be in [0, 1]",
            )
        reliability[annotator] = score
    default_reliability = float(adj_raw.get("default_annotator_reliability", 0.5))
    if not 0.0 <= default_reliability <= 1.0:
        raise BusinessError(
            E_INVALID_CONFIG,
            "adjudication.default_annotator_reliability must be in [0, 1]",
        )
    confidence_threshold = float(adj_raw.get("confidence_threshold", 0.0))
    if not 0.0 <= confidence_threshold <= 1.0:
        raise BusinessError(E_INVALID_CONFIG, "confidence_threshold must be in [0, 1]")
    tie_breaker = str(adj_raw.get("tie_breaker", "strategy_then_lexicographic"))
    if tie_breaker != "strategy_then_lexicographic":
        raise BusinessError(
            E_INVALID_CONFIG,
            "only strategy_then_lexicographic tie breaking is supported",
        )

    v1_raw = _as_mapping(data.get("structure_v1"), "structure_v1")
    confidence_scale = str(v1_raw.get("confidence_scale", "percent"))
    if confidence_scale != "percent":
        raise BusinessError(E_INVALID_CONFIG, "structure_v1.confidence_scale must be percent")
    zero_tokens = str(v1_raw.get("zero_tokens", "unsegmented"))
    if zero_tokens != "unsegmented":
        raise BusinessError(E_INVALID_CONFIG, "structure_v1.zero_tokens must be unsegmented")
    preserve_original = bool(v1_raw.get("preserve_original", True))

    return GovernanceConfig(
        version=version,
        standard_tags=tuple(canonical_tags),
        standard_lookup=standard_lookup,
        aliases=aliases,
        standard_casefold=standard_casefold,
        stem_separators=tuple(stem_separators),
        alignment=AlignmentRules(min_rate, token_comparison, allow_unpaired),
        adjudication=AdjudicationRules(
            strategy_order=strategy_order,
            weights=weights,
            candidate_priority=candidate_priority,
            annotator_reliability=reliability,
            default_annotator_reliability=default_reliability,
            confidence_threshold=confidence_threshold,
            tie_breaker=tie_breaker,
        ),
        v1=V1Rules(confidence_scale, zero_tokens, preserve_original),
        source_path=str(config_path),
        source_digest=sha256_bytes(raw_bytes),
    )


def load_config_dict(data: dict[str, Any], *, source_path: str = "<memory>") -> GovernanceConfig:
    """Useful for tests and the HTTP API."""

    import tempfile

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".yaml", delete=False) as handle:
        json.dump(data, handle, ensure_ascii=False)
        temp_path = handle.name
    try:
        cfg = load_governance_config(temp_path)
        return GovernanceConfig(
            version=cfg.version,
            standard_tags=cfg.standard_tags,
            standard_lookup=cfg.standard_lookup,
            aliases=cfg.aliases,
            standard_casefold=cfg.standard_casefold,
            stem_separators=cfg.stem_separators,
            alignment=cfg.alignment,
            adjudication=cfg.adjudication,
            v1=cfg.v1,
            source_path=source_path,
            source_digest=cfg.source_digest,
        )
    finally:
        Path(temp_path).unlink(missing_ok=True)

