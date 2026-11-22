"""Unicode and tag normalization plus mapping-candidate generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import GovernanceConfig
from .textnorm import collapse_whitespace, nfkc, normalize_label


def canonical_standard_tag(raw: str, config: GovernanceConfig) -> tuple[str, str | None]:
    tag = normalize_label(raw)
    found = config.standard_lookup.get(tag)
    if found is not None:
        return found, None
    return collapse_whitespace(nfkc(raw)), "unknown_new_tag"


@dataclass(frozen=True)
class CandidateResult:
    normalized_old_tag: str
    candidates: tuple[dict[str, Any], ...]
    diagnostics: tuple[dict[str, Any], ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "normalized_old_tag": self.normalized_old_tag,
            "candidates": list(self.candidates),
            "diagnostics": list(self.diagnostics),
        }


def _diagnostic(code: str, message: str, **extra: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {"code": code, "message": message}
    payload.update(extra)
    return payload


def _candidate_sort_key(tag: str, config: GovernanceConfig) -> tuple[int, str]:
    try:
        return config.standard_tags.index(tag), tag
    except ValueError:
        return len(config.standard_tags), tag


def generate_candidates(raw_old_tag: str, config: GovernanceConfig) -> CandidateResult:
    """Normalize a free-form old tag and retain every candidate + provenance.

    Candidate generation never discards an ambiguous or missing alias. Rules
    may add candidates after an alias hit; sources are retained independently.
    """

    normalized = normalize_label(raw_old_tag)
    candidate_sources: dict[str, set[str]] = {}
    candidate_rules: dict[str, set[str]] = {}
    diagnostics: list[dict[str, Any]] = []

    alias_candidates = config.aliases.get(normalized, ())
    for tag in alias_candidates:
        candidate_sources.setdefault(tag, set()).add("alias_table")
    if alias_candidates:
        if len(alias_candidates) > 1:
            diagnostics.append(
                _diagnostic(
                    "multiple_alias_candidates",
                    "alias table supplies multiple candidates; all are retained",
                    candidates=list(alias_candidates),
                )
            )
    else:
        diagnostics.append(
            _diagnostic(
                "alias_missing",
                "normalized old tag is absent from the alias table",
                normalized_old_tag=normalized,
            )
        )

    rule_hits: list[str] = []
    if config.standard_casefold:
        exact = config.standard_lookup.get(normalized)
        if exact is not None:
            candidate_sources.setdefault(exact, set()).add("rule")
            candidate_rules.setdefault(exact, set()).add("standard_casefold")
            rule_hits.append(exact)

    separators = sorted((sep for sep in config.stem_separators if sep), key=len, reverse=True)
    stem_text = normalized
    for separator in separators:
        stem_text = stem_text.replace(separator, " ")
    components = [part for part in stem_text.split() if part]
    for component in components:
        if component == normalized:
            continue
        exact = config.standard_lookup.get(component)
        if exact is not None:
            candidate_sources.setdefault(exact, set()).add("rule")
            candidate_rules.setdefault(exact, set()).add("stem_standard")
            rule_hits.append(exact)

    if not alias_candidates and rule_hits:
        diagnostics.append(
            _diagnostic(
                "alias_missing_rule_fallback",
                "alias table missed the old tag; deterministic rule candidates were retained",
                candidates=sorted(set(rule_hits), key=lambda tag: _candidate_sort_key(tag, config)),
            )
        )
    if not alias_candidates and not rule_hits:
        diagnostics.append(
            _diagnostic(
                "no_candidate",
                "no alias or deterministic rule candidate could be generated",
                normalized_old_tag=normalized,
            )
        )
    if len(candidate_sources) > 1:
        diagnostics.append(
            _diagnostic(
                "multiple_candidates",
                "multiple standard-tag candidates are retained for adjudication",
                candidates=sorted(candidate_sources, key=lambda tag: _candidate_sort_key(tag, config)),
            )
        )

    candidates: list[dict[str, Any]] = []
    for tag in sorted(candidate_sources, key=lambda item: _candidate_sort_key(item, config)):
        sources = candidate_sources[tag]
        candidates.append(
            {
                "tag": tag,
                "source": "alias_table" if "alias_table" in sources else "rule",
                "sources": sorted(sources),
                "rules": sorted(candidate_rules.get(tag, set())),
            }
        )
    return CandidateResult(normalized, tuple(candidates), tuple(diagnostics))

