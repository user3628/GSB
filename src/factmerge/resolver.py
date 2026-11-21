"""Stage 3: exact-value conflict resolution with log-domain confidence."""

from __future__ import annotations

from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, localcontext
from typing import Any, Iterable

from .errors import FactMergeError
from .util import DECIMAL_CONTEXT_PRECISION, decimal_text, dec, digest_without, quantize


def _decimal_ln2(precision: int = 80) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = precision
        z = Decimal(1) / Decimal(3)
        z2 = z * z
        term = z
        total = Decimal(0)
        denominator = 1
        threshold = Decimal(10) ** (-(precision + 5))
        while denominator < 400:
            addition = term / Decimal(denominator)
            total += addition
            if abs(addition) < threshold:
                break
            term *= z2
            denominator += 2
        return +(Decimal(2) * total)


def _ln(value: Decimal) -> Decimal:
    """Natural logarithm using only Decimal arithmetic (no binary float path)."""
    if value <= 0:
        raise ValueError("log domain requires positive components")
    with localcontext() as ctx:
        ctx.prec = 80
        y = Decimal(value)
        if y == 1:
            return Decimal(0)
        exponent = 0
        two = Decimal(2)
        while y < 1:
            y *= two
            exponent -= 1
        while y >= 2:
            y /= two
            exponent += 1
        z = (y - 1) / (y + 1)
        z2 = z * z
        term = z
        total = Decimal(0)
        denominator = 1
        threshold = Decimal(10) ** (-(ctx.prec + 5))
        while denominator < 400:
            addition = term / Decimal(denominator)
            total += addition
            if abs(addition) < threshold:
                break
            term *= z2
            denominator += 2
        result = Decimal(2) * total + Decimal(exponent) * _decimal_ln2(ctx.prec)
        return +result


def _exp(value: Decimal) -> Decimal:
    """Exponential using only Decimal arithmetic and range reduction by ln(2)."""
    with localcontext() as ctx:
        ctx.prec = 80
        x = Decimal(value)
        if x == 0:
            return Decimal(1)
        ln2 = _decimal_ln2(ctx.prec)
        exponent = int((x / ln2).to_integral_value(rounding=ROUND_FLOOR))
        remainder = x - Decimal(exponent) * ln2
        term = Decimal(1)
        total = Decimal(1)
        denominator = 1
        threshold = Decimal(10) ** (-(ctx.prec + 5))
        while denominator < 400:
            term = term * remainder / Decimal(denominator)
            total += term
            if abs(term) < threshold:
                break
            denominator += 1
        result = total * (Decimal(2) ** exponent)
        return +result


def _source_id(raw: dict[str, Any]) -> str:
    for key in ("source", "source_channel", "source_id", "channel"):
        if raw.get(key) not in (None, ""):
            return str(raw[key]).casefold()
    return "unknown"


def _observed_date(raw: dict[str, Any], config: dict[str, Any]) -> date:
    for key in ("observed_at", "source_date", "published_at", "timestamp"):
        value = raw.get(key)
        if value not in (None, ""):
            try:
                return date.fromisoformat(str(value)[:10])
            except ValueError as exc:
                raise FactMergeError(
                    "E_INVALID_FORMAT",
                    f"invalid observation date for claim {raw.get('claim_id')}",
                    claim_id=str(raw.get("claim_id") or ""),
                    entity_id=str(raw.get("entity_id") or ""),
                    details={"field": key, "value": value},
                ) from exc
    return date.fromisoformat(str(config["freshness"]["reference_date"]))


def _annotation(raw: dict[str, Any]) -> Decimal:
    value = raw.get("annotated_confidence", raw.get("confidence", "1"))
    if value in (None, ""):
        return Decimal(1)
    try:
        result = dec(value)
    except (InvalidOperation, ValueError) as exc:
        raise FactMergeError(
            "E_INVALID_FORMAT",
            f"invalid confidence annotation for claim {raw.get('claim_id')}",
            claim_id=str(raw.get("claim_id") or ""),
            entity_id=str(raw.get("entity_id") or ""),
        ) from exc
    if not result.is_finite() or result <= 0 or result > 1:
        raise FactMergeError(
            "E_INVALID_FORMAT",
            "confidence annotation must satisfy 0 < confidence <= 1",
            claim_id=str(raw.get("claim_id") or ""),
            entity_id=str(raw.get("entity_id") or ""),
            details={"value": decimal_text(result)},
        )
    return result


def confidence_for_claim(raw: dict[str, Any], lang: str, config: dict[str, Any]) -> dict[str, Any]:
    source = _source_id(raw)
    prior_text = config["source_priors"].get(source, config["source_priors"].get("unknown", "0.5"))
    prior = dec(prior_text)
    lang_key = (lang or "und").casefold()
    lang_weight = dec(config["language_weights"].get(lang_key, config["language_weights"].get("und", "0.8")))
    observed = _observed_date(raw, config)
    reference = date.fromisoformat(str(config["freshness"]["reference_date"]))
    half_life = dec(config["freshness"]["half_life_days"])
    age_days = max(0, (reference - observed).days)
    with localcontext() as ctx:
        ctx.prec = DECIMAL_CONTEXT_PRECISION
        ratio = dec(age_days) / half_life
        freshness = _exp(_ln(dec("0.5")) * ratio)
    annotation = _annotation(raw)

    with localcontext() as ctx:
        ctx.prec = DECIMAL_CONTEXT_PRECISION
        log_prior = _ln(prior)
        log_language = _ln(lang_weight)
        log_freshness = _ln(freshness)
        log_annotation = _ln(annotation)
        log_score = log_prior + log_language + log_freshness + log_annotation
        score = _exp(log_score)

    log_score_text = decimal_text(quantize(log_score, "0.000000000000000001"))
    score_text = decimal_text(quantize(score, "0.000000000001"))
    return {
        "log_domain": True,
        "components": {
            "source_prior": {
                "source": source,
                "value": decimal_text(prior),
                "log": decimal_text(quantize(log_prior, "0.000000000000000001")),
            },
            "language_weight": {
                "lang": lang_key,
                "value": decimal_text(lang_weight),
                "log": decimal_text(quantize(log_language, "0.000000000000000001")),
            },
            "freshness": {
                "observed_at": observed.isoformat(),
                "reference_date": reference.isoformat(),
                "age_days": age_days,
                "half_life_days": decimal_text(half_life),
                "factor": decimal_text(quantize(freshness, "0.000000000001")),
                "log": decimal_text(quantize(log_freshness, "0.000000000000000001")),
            },
            "annotation": {
                "value": decimal_text(annotation),
                "log": decimal_text(quantize(log_annotation, "0.000000000000000001")),
            },
        },
        "log_score": log_score_text,
        "score": score_text,
    }


def _comparison_key(claim: dict[str, Any], config: dict[str, Any]) -> str:
    value = claim["canonical_value"]
    if value["type"] == "decimal":
        quantum = config["comparison"]["quantum"]
        rounded = quantize(dec(value["value"]), quantum, config["comparison"].get("rounding", "ROUND_HALF_EVEN"))
        return f"{claim['category']}|decimal|{value['canonical_unit']}|{decimal_text(rounded)}"
    if value["type"] == "month_interval":
        start = value["start"]["ordinal"] if value.get("start") else "open"
        end = value["end"]["ordinal"] if value.get("end") else "open"
        return (
            f"{claim['category']}|month_interval|{start}|{end}|"
            f"{str(value['start_closed']).lower()}|{str(value['end_closed']).lower()}"
        )
    return f"{claim['category']}|unknown|{value.get('value')}"


def _comparison_display(claim: dict[str, Any], config: dict[str, Any]) -> Any:
    value = claim["canonical_value"]
    if value["type"] == "decimal":
        rounded = quantize(
            dec(value["value"]),
            config["comparison"]["quantum"],
            config["comparison"].get("rounding", "ROUND_HALF_EVEN"),
        )
        return {
            "type": "decimal",
            "value": decimal_text(rounded),
            "canonical_unit": value["canonical_unit"],
            "precision_digits": int(config["comparison"]["precision_digits"]),
        }
    return {
        "type": value["type"],
        "value": value["value"],
        "start": value.get("start"),
        "end": value.get("end"),
        "start_closed": value.get("start_closed"),
        "end_closed": value.get("end_closed"),
    }


def _freshness_ord(raw: dict[str, Any], config: dict[str, Any]) -> int:
    value = _observed_date(raw, config)
    return value.year * 10000 + value.month * 100 + value.day


def _context(
    claim: dict[str, Any],
    raw: dict[str, Any],
    config: dict[str, Any],
) -> dict[str, Any]:
    source = _source_id(raw)
    lang = str(claim.get("lang") or raw.get("lang") or "und").casefold()
    return {
        "claim": claim,
        "claim_id": claim["claim_id"],
        "category": claim["category"],
        "confidence": confidence_for_claim(raw, lang, config),
        "source_priority": int(config.get("source_priority", {}).get(source, config.get("source_priority", {}).get("unknown", 0))),
        "language_priority": int(config.get("language_priority", {}).get(lang, config.get("language_priority", {}).get("und", 0))),
        "freshness_ord": _freshness_ord(raw, config),
        "comparison_key": _comparison_key(claim, config),
        "comparison_display": _comparison_display(claim, config),
    }


def _arbitration_key(context: dict[str, Any], rules: list[str]) -> tuple[Any, ...]:
    key: list[Any] = []
    for rule in rules:
        if rule == "confidence_desc":
            key.append(-dec(context["confidence"]["log_score"]))
        elif rule == "source_priority_desc":
            key.append(-context["source_priority"])
        elif rule == "language_priority_desc":
            key.append(-context["language_priority"])
        elif rule == "freshness_desc":
            key.append(-context["freshness_ord"])
        elif rule == "claim_id_asc":
            key.append(context["claim_id"])
        else:
            raise FactMergeError("E_INVALID_FORMAT", f"unknown arbitration rule: {rule}")
    return tuple(key)


def _tie_trace(sorted_contexts: list[dict[str, Any]], rules: list[str]) -> dict[str, Any]:
    if len(sorted_contexts) < 2:
        return {"required": False, "rules": rules, "equal_prefix": [], "deciding_rule": None}
    first = sorted_contexts[0]
    second = sorted_contexts[1]
    values = {
        "confidence_desc": first["confidence"]["log_score"] == second["confidence"]["log_score"],
        "source_priority_desc": first["source_priority"] == second["source_priority"],
        "language_priority_desc": first["language_priority"] == second["language_priority"],
        "freshness_desc": first["freshness_ord"] == second["freshness_ord"],
        "claim_id_asc": first["claim_id"] == second["claim_id"],
    }
    equal_prefix: list[str] = []
    deciding = None
    for rule in rules:
        if values.get(rule, False):
            equal_prefix.append(rule)
        else:
            deciding = rule
            break
    return {
        "required": bool(equal_prefix),
        "rules": rules,
        "equal_prefix": equal_prefix,
        "deciding_rule": deciding,
    }


def _decision_base(context: dict[str, Any] | None, claim_id: str, category: str) -> dict[str, Any]:
    return {
        "claim_id": claim_id,
        "category": category,
        "confidence": context["confidence"] if context else None,
        "comparison_value": context["comparison_display"] if context else None,
        "comparison_key": context["comparison_key"] if context else None,
    }


def resolve_entity(
    entity_id: str,
    normalized_claims: Iterable[dict[str, Any]],
    parse_errors: Iterable[dict[str, Any]],
    raw_claims: dict[str, dict[str, Any]],
    config: dict[str, Any],
) -> dict[str, Any]:
    normalized = sorted(list(normalized_claims), key=lambda claim: claim["claim_id"])
    errors = sorted(list(parse_errors), key=lambda err: str(err.get("claim_id", "")))
    decisions: dict[str, dict[str, Any]] = {}
    by_category: dict[str, list[dict[str, Any]]] = {}
    category_errors: dict[str, list[dict[str, Any]]] = {}

    contexts: dict[str, dict[str, Any]] = {}
    for claim in normalized:
        raw = raw_claims.get(claim["claim_id"], {})
        context = _context(claim, raw, config)
        contexts[claim["claim_id"]] = context
        by_category.setdefault(claim["category"], []).append(context)

    for error in errors:
        claim_id = str(error.get("claim_id") or "")
        category = str(error.get("category") or "unknown")
        category_errors.setdefault(category, []).append(error)
        decisions[claim_id] = {
            "claim_id": claim_id,
            "entity_id": entity_id,
            "category": category,
            "decision": "demoted",
            "reason_code": str(error.get("code", "E_PARSE_AMBIGUOUS")),
            "reason": str(error.get("message", "claim could not be parsed")),
            "winner_claim_id": None,
            "confidence": None,
            "comparison_value": None,
            "comparison_key": None,
            "tie_break": None,
            "parse_error": error,
        }

    category_results: list[dict[str, Any]] = []
    accepted_all: list[dict[str, Any]] = []
    demoted_all: list[dict[str, Any]] = []
    all_categories = sorted(set(by_category) | set(category_errors))
    rules = list(config["arbitration"])

    for category in all_categories:
        category_contexts = sorted(by_category.get(category, []), key=lambda item: item["claim_id"])
        valid = [ctx for ctx in category_contexts if ctx["claim"]["status"] == "ok"]
        review = [ctx for ctx in category_contexts if ctx["claim"]["status"] != "ok"]
        sorted_valid = sorted(valid, key=lambda ctx: _arbitration_key(ctx, rules))
        groups: dict[str, list[dict[str, Any]]] = {}
        for context in sorted_valid:
            groups.setdefault(context["comparison_key"], []).append(context)
        conflict_detected = len(groups) > 1
        winner = sorted_valid[0] if sorted_valid else None
        winner_id = winner["claim_id"] if winner else None
        tie_trace = _tie_trace(sorted_valid, rules)

        value_groups: list[dict[str, Any]] = []
        for group_key, members in sorted(groups.items(), key=lambda item: item[0]):
            ordered = sorted(members, key=lambda ctx: _arbitration_key(ctx, rules))
            value_groups.append(
                {
                    "comparison_key": group_key,
                    "value": ordered[0]["comparison_display"],
                    "claim_ids": sorted(member["claim_id"] for member in members),
                    "best_claim_id": ordered[0]["claim_id"],
                }
            )

        if winner is not None:
            accepted_decision = {
                "claim_id": winner_id,
                "entity_id": entity_id,
                "category": category,
                "decision": "accepted",
                "reason_code": "ACCEPTED_HIGHEST_CONFIDENCE",
                "reason": "highest deterministic confidence rank within the entity/category",
                "winner_claim_id": winner_id,
                "confidence": winner["confidence"],
                "comparison_value": winner["comparison_display"],
                "comparison_key": winner["comparison_key"],
                "tie_break": tie_trace if tie_trace["required"] else None,
                "supporting_claim_ids": sorted(
                    group["claim_id"]
                    for group in groups.get(winner["comparison_key"], [])
                    if group["claim_id"] != winner_id
                ),
            }
            decisions[winner_id] = accepted_decision
            accepted_all.append(
                {
                    "category": category,
                    "claim_id": winner_id,
                    "value": winner["comparison_display"],
                    "confidence": winner["confidence"],
                    "supporting_claim_ids": accepted_decision["supporting_claim_ids"],
                }
            )

            for context in category_contexts:
                claim_id = context["claim_id"]
                if claim_id == winner_id:
                    continue
                same_group = context["comparison_key"] == winner["comparison_key"] and context["claim"]["status"] == "ok"
                if context["claim"]["status"] != "ok":
                    codes = [issue["code"] for issue in context["claim"].get("review_issues", [])]
                    reason_code = "REVIEW_REQUIRED"
                    reason = "claim normalized to review status: " + ", ".join(codes or ["UNSPECIFIED"])
                elif same_group:
                    reason_code = "EQUIVALENT_SUPPORT"
                    reason = "same normalized value as the accepted claim and retained as supporting evidence"
                else:
                    reason_code = "CONFLICT_LOWER_CONFIDENCE"
                    reason = "conflicting exact value lost deterministic confidence arbitration"
                decision = {
                    "claim_id": claim_id,
                    "entity_id": entity_id,
                    "category": category,
                    "decision": "demoted",
                    "reason_code": reason_code,
                    "reason": reason,
                    "winner_claim_id": winner_id,
                    "confidence": context["confidence"],
                    "comparison_value": context["comparison_display"],
                    "comparison_key": context["comparison_key"],
                    "tie_break": None,
                    "review_issues": context["claim"].get("review_issues", []),
                }
                decisions[claim_id] = decision
                demoted_all.append(
                    {
                        "category": category,
                        "claim_id": claim_id,
                        "reason_code": reason_code,
                        "reason": reason,
                        "winner_claim_id": winner_id,
                    }
                )
        else:
            for context in category_contexts:
                codes = [issue["code"] for issue in context["claim"].get("review_issues", [])]
                decision = {
                    "claim_id": context["claim_id"],
                    "entity_id": entity_id,
                    "category": category,
                    "decision": "demoted",
                    "reason_code": "REVIEW_REQUIRED",
                    "reason": "no claim in this category passed normalization; review required: "
                    + ", ".join(codes or ["UNSPECIFIED"]),
                    "winner_claim_id": None,
                    "confidence": context["confidence"],
                    "comparison_value": context["comparison_display"],
                    "comparison_key": context["comparison_key"],
                    "tie_break": None,
                    "review_issues": context["claim"].get("review_issues", []),
                }
                decisions[context["claim_id"]] = decision
                demoted_all.append(
                    {
                        "category": category,
                        "claim_id": context["claim_id"],
                        "reason_code": "REVIEW_REQUIRED",
                        "reason": decision["reason"],
                        "winner_claim_id": None,
                    }
                )

        for error in sorted(category_errors.get(category, []), key=lambda item: str(item.get("claim_id", ""))):
            claim_id = str(error.get("claim_id") or "")
            demoted_all.append(
                {
                    "category": category,
                    "claim_id": claim_id,
                    "reason_code": str(error.get("code", "E_PARSE_AMBIGUOUS")),
                    "reason": str(error.get("message", "claim could not be parsed")),
                    "winner_claim_id": None,
                }
            )

        category_results.append(
            {
                "category": category,
                "status": "resolved" if winner is not None else ("review_required" if category_contexts else "parse_error"),
                "conflict_detected": conflict_detected,
                "distinct_value_count": len(groups),
                "accepted_claim_id": winner_id,
                "value_groups": value_groups,
                "claim_ids": sorted(context["claim_id"] for context in category_contexts)
                + sorted(str(error.get("claim_id", "")) for error in category_errors.get(category, [])),
                "review_claim_ids": sorted(context["claim_id"] for context in review),
                "parse_error_claim_ids": sorted(str(error.get("claim_id", "")) for error in category_errors.get(category, [])),
                "tie_break": tie_trace if tie_trace["required"] else None,
            }
        )

    for claim_id in sorted(decisions):
        decisions[claim_id]["digest"] = digest_without(decisions[claim_id], "digest")

    result = {
        "entity_id": entity_id,
        "status": "review_required" if not accepted_all else ("resolved_with_reviews" if demoted_all else "resolved"),
        "categories": category_results,
        "decisions": [decisions[claim_id] for claim_id in sorted(decisions)],
        "accepted": accepted_all,
        "demoted": demoted_all,
        "conflict_detected": any(item["conflict_detected"] for item in category_results),
    }
    result["digest"] = digest_without(result, "digest")
    return result
