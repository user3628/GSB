from __future__ import annotations

import datetime as dt
from typing import Any

from .errors import BusinessError


_ALLOWED_ORDER = {
    "effective_date",
    "clause_priority",
    "clause_no",
    "unit_index",
    "source_start",
    "candidate_id",
}


def parse_effective_date(value: str | None, contract_id: str, config: dict[str, Any]) -> tuple[str, int]:
    raw = config.get("default_effective_date", "1970-01-01") if value in (None, "") else value
    if not isinstance(raw, str) or raw != raw.strip():
        raise BusinessError(
            "E_INVALID_DATE",
            contract_id,
            "arbitrate",
            "effective date is not a valid ISO date",
            {"effective_date": raw},
        )
    try:
        parsed = dt.date.fromisoformat(raw)
    except ValueError as exc:
        raise BusinessError(
            "E_INVALID_DATE",
            contract_id,
            "arbitrate",
            "effective date is not a valid ISO date",
            {"effective_date": raw},
        ) from exc
    if parsed.isoformat() != raw:
        raise BusinessError(
            "E_INVALID_DATE",
            contract_id,
            "arbitrate",
            "effective date must use YYYY-MM-DD",
            {"effective_date": raw},
        )
    return raw, parsed.toordinal()


def _priority(candidate: dict[str, Any], config: dict[str, Any]) -> tuple[int, int, int]:
    clause_map = config.get("clause_priority", {})
    template_map = config.get("template_priority", {})
    default = int(config.get("default_clause_priority", 0))
    clause_priority = int(clause_map.get(str(candidate["clause_no"]), default))
    template_priority = int(template_map.get(str(candidate.get("template_id", "")), 0))
    return clause_priority + template_priority, clause_priority, template_priority


def _field_values(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "effective_date": candidate["_effective_date"],
        "effective_date_ordinal": candidate["_effective_ordinal"],
        "clause_priority": candidate["_clause_priority"],
        "clause_no": candidate["clause_no"],
        "unit_index": candidate["unit_index"],
        "source_start": candidate["source_start"],
        "candidate_id": candidate["candidate_id"],
    }


def _sort_key(candidate: dict[str, Any], order: list[str], descending: set[str]) -> tuple[Any, ...]:
    values = _field_values(candidate)
    key: list[Any] = []
    for field in order:
        value = values["effective_date_ordinal"] if field == "effective_date" else values[field]
        key.append(-value if field in descending else value)
    for field in ("effective_date", "clause_priority", "clause_no", "unit_index", "source_start", "candidate_id"):
        if field not in order:
            value = values["effective_date_ordinal"] if field == "effective_date" else values[field]
            key.append(-value if field in descending else value)
    return tuple(key)


def _rank_record(candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "effective_date": candidate["_effective_date"],
        "clause_priority": candidate["_clause_priority"],
        "clause_no": candidate["clause_no"],
        "unit_index": candidate["unit_index"],
        "source_start": candidate["source_start"],
        "candidate_id": candidate["candidate_id"],
    }


def _reason(winner: dict[str, Any], loser: dict[str, Any], descending: set[str]) -> str:
    comparisons = [
        ("effective_date", "older_effective_date"),
        ("clause_priority", "lower_clause_priority"),
        ("clause_no", "higher_clause_no"),
        ("unit_index", "later_unit_index"),
        ("source_start", "later_source_offset"),
        ("candidate_id", "lexicographically_larger_candidate_id"),
    ]
    for field, message in comparisons:
        if field == "effective_date":
            winner_value = winner["_effective_date"]
            loser_value = loser["_effective_date"]
        elif field == "clause_priority":
            winner_value = winner["_clause_priority"]
            loser_value = loser["_clause_priority"]
        else:
            winner_value = winner.get(field)
            loser_value = loser.get(field)
        if winner_value == loser_value:
            continue
        if field in descending:
            better = winner_value > loser_value
        else:
            better = winner_value < loser_value
        if better:
            return message
    return "deterministic_final_tie_breaker"


def resolve_contract(
    contract_id: str,
    candidates: list[dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    order = list(config["order"])
    if not order or not set(order).issubset(_ALLOWED_ORDER):
        raise ValueError("unsupported arbitration order")
    descending = set(config.get("descending", []))
    valid = [candidate for candidate in candidates if candidate.get("valid") and candidate.get("normalized_value")]
    enriched: list[dict[str, Any]] = []
    for candidate in valid:
        copied = dict(candidate)
        date_text, date_ordinal = parse_effective_date(copied.get("effective_date"), contract_id, config)
        combined, clause_priority, template_priority = _priority(copied, config)
        copied["_effective_date"] = date_text
        copied["_effective_ordinal"] = date_ordinal
        copied["_clause_priority"] = combined
        copied["_clause_priority_component"] = clause_priority
        copied["_template_priority_component"] = template_priority
        copied["_sort_key"] = _sort_key(copied, order, descending)
        enriched.append(copied)

    grouped: dict[str, list[dict[str, Any]]] = {}
    for candidate in enriched:
        grouped.setdefault(candidate["normalized_value"], []).append(candidate)
    for value in grouped:
        grouped[value].sort(key=lambda item: item["_sort_key"])

    winners_by_element: dict[str, dict[str, Any]] = {}
    for element_type in sorted({candidate["element_type"] for candidate in enriched}):
        element_candidates = [candidate for candidate in enriched if candidate["element_type"] == element_type]
        values: dict[str, list[dict[str, Any]]] = {}
        for candidate in element_candidates:
            values.setdefault(candidate["normalized_value"], []).append(candidate)
        for value in values:
            values[value].sort(key=lambda item: item["_sort_key"])
        ranked_values = sorted(values, key=lambda value: values[value][0]["_sort_key"])
        winner = values[ranked_values[0]][0]
        conflict = len(ranked_values) > 1
        eliminated: list[dict[str, Any]] = []
        if conflict:
            for losing_value in ranked_values[1:]:
                representative = values[losing_value][0]
                eliminated.append(
                    {
                        "normalized_value": losing_value,
                        "candidate_ids": sorted(candidate["candidate_id"] for candidate in values[losing_value]),
                        "representative": {
                            key: value
                            for key, value in representative.items()
                            if not key.startswith("_")
                        },
                        "rank": _rank_record(representative),
                        "winner_rank": _rank_record(winner),
                        "reason": _reason(winner, representative, descending),
                    }
                )

        evaluated = []
        for candidate in sorted(element_candidates, key=lambda item: item["_sort_key"]):
            cleaned = {key: value for key, value in candidate.items() if not key.startswith("_")}
            cleaned["rank"] = _rank_record(candidate)
            evaluated.append(cleaned)
        winners_by_element[element_type] = {
            "type": "resolved_element",
            "contract_id": contract_id,
            "element_type": element_type,
            "value": winner["normalized_value"],
            "winner_candidate_id": winner["candidate_id"],
            "conflict": conflict,
            "policy": {
                "order": order,
                "descending": sorted(descending),
                "tie_breaker": "stable candidate_id after configured fields",
            },
            "evaluated_candidates": evaluated,
            "eliminated": eliminated,
        }
    return winners_by_element

